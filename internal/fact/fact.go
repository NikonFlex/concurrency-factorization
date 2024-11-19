package fact

import (
	"errors"
	"fmt"
	"io"
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
	context := executionContext{done, errorCh, writer, checkedConfig}

	// Generate channels for processing.
	wgGeneration := &sync.WaitGroup{}
	numbersCh := generateNumbers(numbers, wgGeneration, context)

	wgFactorization := &sync.WaitGroup{}
	resultsCh := startFactorization(wgFactorization, numbersCh, context)

	wgWriting := &sync.WaitGroup{}
	startWriting(wgWriting, resultsCh, context)

	// Wait for all workers to finish.
	wgFactorization.Wait()
	close(resultsCh)
	wgWriting.Wait()
	wgGeneration.Wait()

	// Check for errors or cancellation signals.
	select {
	case err := <-errorCh:
		return err
	case <-done:
		return ErrFactorizationCancelled
	default:
		return nil
	}
}

// checkConfig validates and returns the configuration, applying defaults if necessary.
func checkConfig(config ...Config) (Config, error) {
	if config == nil {
		config = make([]Config, 1)
		return Config{FactorizationWorkers: 1, WriteWorkers: 1}, nil
	}
	if len(config) == 0 || config[0].FactorizationWorkers < 0 || config[0].WriteWorkers < 0 {
		return Config{}, errors.New("invalid configuration")
	}
	return config[0], nil
}

// generateNumbers sends the input numbers to a channel for processing by workers.
func generateNumbers(numbers []int, wg *sync.WaitGroup, context executionContext) <-chan int {
	numbersCh := make(chan int)
	wg.Add(1)
	go func() {
		defer close(numbersCh)
		defer wg.Done()
		for _, num := range numbers {
			select {
			case <-context.done:
				return
			case <-context.errorCh:
				return
			case numbersCh <- num: // Send number to the channel.
			}
		}
	}()
	return numbersCh
}

// startFactorization launches factorization workers to process numbers from the channel.
func startFactorization(wg *sync.WaitGroup, numbersCh <-chan int, context executionContext) chan *factorizedNumber {
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
func startWriting(wg *sync.WaitGroup, resultsCh chan *factorizedNumber, context executionContext) {
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
						case context.errorCh <- errors.Join(ErrWriterInteraction, err):
							return
						}
					}
				}
			}
		}()
	}
}

// factorize computes the prime factorization of a number.
func factorize(n int) factorizedNumber {
	result := factorizedNumber{number: n}

	if n < 0 {
		result.factors = append(result.factors, -1)
		n = -n
	}

	if n == 0 {
		result.factors = append(result.factors, 0)
		return result
	}

	if n == 1 {
		result.factors = append(result.factors, 1)
		return result
	}

	// factorizing in O(sqrt)
	for i := 2; i*i <= n || -i*i >= n; i++ {
		for n%i == 0 {
			result.factors = append(result.factors, i)
			n /= i
		}
	}

	if n < 0 {
		n = -n
	}
	if n > 1 {
		result.factors = append(result.factors, n)
	}

	return result
}

// writeResult writes a formatted factorized number to the output writer.
func writeResult(num *factorizedNumber, writer io.Writer) error {
	output := formatFactorizedNumber(num) + "\n"
	_, err := writer.Write([]byte(output))
	return err
}

// formatFactorizedNumber formats a factorized number as a string.
func formatFactorizedNumber(num *factorizedNumber) string {
	factorStrings := make([]string, len(num.factors))
	for i, factor := range num.factors {
		factorStrings[i] = strconv.Itoa(factor)
	}
	return fmt.Sprintf("%d = %s", num.number, strings.Join(factorStrings, " * "))
}
