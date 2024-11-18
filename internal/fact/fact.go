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
	FactorizationWorkers int
	WriteWorkers         int
}

// Factorization interface represents a concurrent prime factorization task with configurable workers.
// Thread safety and error handling are implemented as follows:
// - The provided writer must be thread-safe to handle concurrent writes from multiple workers.
// - Output uses '\n' for newlines.
// - Factorization has a time complexity of O(sqrt(n)) per number.
// - If an error occurs while writing to the writer, early termination is triggered across all workers.
type Factorization interface {
	// Do performs factorization on a list of integers, writing the results to an io.Writer.
	// - done: a channel to signal early termination.
	// - numbers: the list of integers to factorize.
	// - writer: the io.Writer where factorization results are output.
	// - config: optional worker configuration.
	// Returns an error if the process is cancelled or if a writer error occurs.
	Do(done <-chan struct{}, numbers []int, writer io.Writer, config ...Config) error
}

type factorizedNumber struct {
	number  int
	factors []int
}

// factorizationImpl provides an implementation for the Factorization interface.
type factorizationImpl struct{}

func New() *factorizationImpl {
	return &factorizationImpl{}
}

func (f *factorizationImpl) Do(
	done <-chan struct{},
	numbers []int,
	writer io.Writer,
	config ...Config,
) error {
	if config == nil {
		config = make([]Config, 1)
		config[0] = Config{FactorizationWorkers: 1, WriteWorkers: 1}
	}
	if len(config) == 0 || config[0].FactorizationWorkers < 0 || config[0].WriteWorkers < 0 {
		return errors.New("config is unsupported")
	}

	errorCh := make(chan error, 10)
	numbersCh := generateNumbers(numbers, done)

	resultsCh := make(chan factorizedNumber)
	var wgFactorization sync.WaitGroup

	for i := 0; i < config[0].FactorizationWorkers; i++ {
		wgFactorization.Add(1)
		go func() {
			defer wgFactorization.Done()
			for {
				select {
				case <-done:
					return
				case <-errorCh:
					return
				case num, ok := <-numbersCh:
					if !ok {
						return
					}
					result := factorize(num)
					select {
					case <-done:
						return
					case <-errorCh:
						return
					case resultsCh <- result:
					}
				}
			}
		}()
	}

	var wgWriting sync.WaitGroup
	for i := 0; i < config[0].WriteWorkers; i++ {
		wgWriting.Add(1)
		go func() {
			defer wgWriting.Done()
			for {
				select {
				case <-done:
					return
				case <-errorCh:
					return
				case result, ok := <-resultsCh:
					if !ok {
						return
					}

					if err := writeResult(result, writer); err != nil {
						select {
						case <-done:
							return
						case errorCh <- ErrWriterInteraction:
							return
						default:
							return
						}
					}
				}
			}
		}()
	}

	wgFactorization.Wait()
	close(resultsCh)
	wgWriting.Wait()

	select {
	case err := <-errorCh:
		return err
	case <-done:
		return ErrFactorizationCancelled
	default:
		return nil
	}
}

func generateNumbers(numbers []int, done <-chan struct{}) <-chan int {
	numbersCh := make(chan int)

	go func() {
		defer close(numbersCh)
		for _, num := range numbers {
			select {
			case <-done:
				return
			case numbersCh <- num:
			}
		}
	}()

	return numbersCh
}

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

	// Проверяем делители до корня из n
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

func writeResult(num factorizedNumber, writer io.Writer) error {
	output := formatFactorizedNumber(num) + "\n"
	_, err := writer.Write([]byte(output))
	return err
}

func formatFactorizedNumber(num factorizedNumber) string {
	factorStrings := make([]string, len(num.factors))
	for i, factor := range num.factors {
		factorStrings[i] = strconv.Itoa(factor)
	}

	return fmt.Sprintf("%d = %s", num.number, strings.Join(factorStrings, " * "))
}
