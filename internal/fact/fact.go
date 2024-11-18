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
	FactorizationWorkers int // Number of workers for performing factorization.
	WriteWorkers         int // Number of workers for writing results.
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

// factorizedNumber represents the result of factorization for a single number.
type factorizedNumber struct {
	number  int   // The original number that was factorized.
	factors []int // The prime factors of the number.
}

// factorizationImpl provides an implementation for the Factorization interface.
type factorizationImpl struct{}

// New creates a new instance of the factorizationImpl.
func New() *factorizationImpl {
	return &factorizationImpl{}
}

// Do performs the concurrent factorization and result writing based on the given configuration.
func (f *factorizationImpl) Do(
	done <-chan struct{},
	numbers []int,
	writer io.Writer,
	config ...Config,
) error {
	// Set default configuration if none is provided.
	if config == nil {
		config = make([]Config, 1)
		config[0] = Config{FactorizationWorkers: 1, WriteWorkers: 1}
	}
	if len(config) == 0 || config[0].FactorizationWorkers < 0 || config[0].WriteWorkers < 0 {
		return errors.New("config is unsupported")
	}

	errorCh := make(chan error, config[0].WriteWorkers) // Channel for capturing errors during execution.
	numbersCh := generateNumbers(numbers, done)         // Channel for feeding numbers to workers.

	resultsCh := make(chan factorizedNumber) // Channel for passing factorization results.
	var wgFactorization sync.WaitGroup       // WaitGroup for factorization workers.

	// Start factorization workers.
	for i := 0; i < config[0].FactorizationWorkers; i++ {
		wgFactorization.Add(1)
		go func() {
			defer wgFactorization.Done()
			for {
				select {
				case <-done: // Exit if done signal is received.
					return
				case <-errorCh: // Exit if an error is encountered.
					return
				case num, ok := <-numbersCh: // Receive number for factorization.
					if !ok {
						return
					}
					result := factorize(num) // Perform factorization.
					select {
					case <-done:
						return
					case resultsCh <- result: // Send result to results channel.
					}
				}
			}
		}()
	}

	var wgWriting sync.WaitGroup // WaitGroup for writing workers.
	// Start writing workers.
	for i := 0; i < config[0].WriteWorkers; i++ {
		wgWriting.Add(1)
		go func() {
			defer wgWriting.Done()
			for {
				select {
				case <-done: // Exit if done signal is received.
					return
				case result, ok := <-resultsCh: // Receive result to write.
					if !ok {
						return
					}

					if err := writeResult(result, writer); err != nil {
						select {
						case <-done:
							return
						case errorCh <- ErrWriterInteraction: // Send writer error to error channel.
							return
						default:
							return
						}
					}
				}
			}
		}()
	}

	// Wait for all workers to finish.
	wgFactorization.Wait()
	close(resultsCh)
	wgWriting.Wait()

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

// generateNumbers creates a channel to stream numbers to workers.
func generateNumbers(numbers []int, done <-chan struct{}) <-chan int {
	numbersCh := make(chan int)

	go func() {
		defer close(numbersCh)
		for _, num := range numbers {
			select {
			case <-done: // Exit if done signal is received.
				return
			case numbersCh <- num: // Send number to channel.
			}
		}
	}()

	return numbersCh
}

// factorize performs prime factorization on a single number.
func factorize(n int) factorizedNumber {
	result := factorizedNumber{number: n}

	if n < 0 { // Handle negative numbers.
		result.factors = append(result.factors, -1)
		n = -n
	}

	if n == 0 { // Handle zero.
		result.factors = append(result.factors, 0)
		return result
	}

	if n == 1 { // Handle one.
		result.factors = append(result.factors, 1)
		return result
	}

	// Check divisors up to the square root of n.
	for i := 2; i*i <= n; i++ {
		for n%i == 0 { // Factor out i as long as it divides n.
			result.factors = append(result.factors, i)
			n /= i
		}
	}

	if n > 1 { // If n is still greater than 1, it is a prime factor.
		result.factors = append(result.factors, n)
	}

	return result
}

// writeResult writes the factorized number to the writer.
func writeResult(num factorizedNumber, writer io.Writer) error {
	output := formatFactorizedNumber(num) + "\n" // Format output string.
	_, err := writer.Write([]byte(output))       // Write to the provided writer.
	return err
}

// formatFactorizedNumber formats a factorized number as a string.
func formatFactorizedNumber(num factorizedNumber) string {
	factorStrings := make([]string, len(num.factors))
	for i, factor := range num.factors {
		factorStrings[i] = strconv.Itoa(factor) // Convert factors to strings.
	}

	// Join factors with '*' and format the final string.
	return fmt.Sprintf("%d = %s", num.number, strings.Join(factorStrings, " * "))
}
