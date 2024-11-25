package fact

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
)

var (
	// ErrFactorizationCancelled is returned when the factorization process is cancelled via the done channel.
	ErrFactorizationCancelled = errors.New("cancelled")

	// ErrWriterInteraction is returned if an error occurs while interacting with the writer
	// triggering early termination.
	ErrWriterInteraction = errors.New("writer interaction")
)

// Config defines the configuration for factorization and write workers.
type Config struct {
	FactorizationWorkers int // Number of workers for factorization.
	WriteWorkers         int // Number of workers for writing results.
}

// Factorization interface represents a concurrent prime factorization task with configurable workers.
// The implementation is thread-safe and provides error handling as follows:
// - The writer must be thread-safe to handle concurrent writes.
// - Output uses '\n' for line breaks.
// - Prime factorization has a time complexity of O(sqrt(n)) per number.
// - Any write error triggers early termination across all workers.
type Factorization interface {
	// Do performs factorization on a list of integers and writes the results to an io.Writer.
	// - done: a channel to signal early termination.
	// - numbers: the list of integers to factorize.
	// - writer: the io.Writer to output the results.
	// - config: optional configuration for the workers.
	// Returns an error if cancelled or if a write error occurs.
	Do(done <-chan struct{}, numbers []int, writer io.Writer, config ...Config) error
}

// factorizedNumber represents the result of factorization for a single number.
type factorizedNumber struct {
	number  int   // The original number.
	factors []int // The prime factors of the number.
}

// executionContext bundles parameters for the execution flow.
type executionContext struct {
	done    <-chan struct{} // Channel to signal cancellation.
	errorCh chan error      // Channel for reporting errors.
	writer  io.Writer       // Writer for output.
	config  Config          // Configuration for workers.
	err     error
}

// factorizationImpl provides an implementation for the Factorization interface.
type factorizationImpl struct{}

// New creates a new instance of the factorization implementation.
func New() *factorizationImpl {
	return &factorizationImpl{}
}

// Do performs concurrent factorization and writing based on the provided configuration.
func (f *factorizationImpl) Do(
	done <-chan struct{},
	numbers []int,
	writer io.Writer,
	config ...Config,
) error {
	// Validate the configuration or apply defaults.
	checkedConfig, err := checkConfig(config...)
	if err != nil {
		return err
	}

	errorCh := make(chan error, checkedConfig.WriteWorkers) // Channel to capture errors.
	context := &executionContext{done, errorCh, writer, checkedConfig, nil}
	numbersCh := make(chan int)

	wgFactorization := &sync.WaitGroup{}
	resultsCh := startFactorization(wgFactorization, numbersCh, context)

	wgWriting := &sync.WaitGroup{}
	startWriting(wgWriting, resultsCh, context)

	generateNumbers(numbers, numbersCh, context)

	// Wait for all workers to finish.
	wgFactorization.Wait()
	close(resultsCh)
	wgWriting.Wait()

	// Check for errors or cancellation signals.
	select {
	case <-errorCh:
		return context.err
	case <-done:
		return ErrFactorizationCancelled
	default:
		return nil
	}
}

// checkConfig validates and returns the configuration, applying defaults if necessary.
func checkConfig(config ...Config) (Config, error) {
	if len(config) == 0 {
		return Config{FactorizationWorkers: 1, WriteWorkers: 1}, nil
	}
	if config[0].FactorizationWorkers < 0 || config[0].WriteWorkers < 0 {
		return Config{}, errors.New("invalid configuration")
	}
	return config[0], nil
}

// generateNumbers sends the input numbers to a channel for processing by workers.
func generateNumbers(numbers []int, numberCh chan int, context *executionContext) {
	defer close(numberCh)
	for _, num := range numbers {
		select {
		case <-context.done:
			return
		case <-context.errorCh:
			return
		case numberCh <- num: // Send number to the channel.
		}
	}
}

// startFactorization launches factorization workers to process numbers from the channel.
func startFactorization(wg *sync.WaitGroup, numbersCh <-chan int, context *executionContext) chan *factorizedNumber {
	resultsCh := make(chan *factorizedNumber)
	for i := 0; i < context.config.FactorizationWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-context.done:
					return
				case <-context.errorCh:
					return
				case num, ok := <-numbersCh:
					if !ok {
						return
					}
					result := factorize(num) // Perform factorization.
					select {
					case <-context.done:
						return
					case <-context.errorCh:
						return
					case resultsCh <- &result: // Send result to results channel.
					}
				}
			}
		}()
	}
	return resultsCh
}

// startWriting launches workers to write factorization results to the output writer.
func startWriting(wg *sync.WaitGroup, resultsCh chan *factorizedNumber, context *executionContext) {
	var once sync.Once
	for i := 0; i < context.config.WriteWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-context.done:
					return
				case <-context.errorCh:
					return
				case result, ok := <-resultsCh:
					if !ok {
						return
					}
					if err := writeResult(result, context.writer); err != nil {
						select {
						case <-context.done:
							return
						default:
							once.Do(func() { onErrorHappened(err, context) })
						}
					}
				}
			}
		}()
	}
}

func onErrorHappened(err error, context *executionContext) {
	close(context.errorCh)
	context.err = errors.Join(ErrWriterInteraction, err)
}

// factorize computes the prime factorization of a number.
func factorize(n int) factorizedNumber {
	result := factorizedNumber{number: n}

	if n == math.MinInt {
		result.factors = append(result.factors, -1)
		result.factors = append(result.factors, 2)
		n = -(n / 2)
	}
	if n < 0 {
		result.factors = append(result.factors, -1)
		n = -n
	}

	if n == 0 || n == 1 {
		result.factors = append(result.factors, n)
		return result
	}

	// factorizing in O(sqrt)
	for i := 2; i*i <= n; i++ {
		for n%i == 0 {
			result.factors = append(result.factors, i)
			n /= i
		}
	}

	if n > 1 {
		result.factors = append(result.factors, n)
	}

	return result
}

// writeResult writes a formatted factorized number to the output writer.
func writeResult(num *factorizedNumber, writer io.Writer) error {
	factorStrings := make([]string, len(num.factors))
	for i, factor := range num.factors {
		factorStrings[i] = strconv.Itoa(factor)
	}
	_, err := fmt.Fprintf(writer, "%d = %s\n", num.number, strings.Join(factorStrings, " * "))
	return err
}
