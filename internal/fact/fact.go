package fact

import (
	"errors"
	"io"
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

// factorizationImpl provides an implementation for the Factorization interface.
type factorizationImpl struct{}

func New() *factorizationImpl {
	panic("not implemented")
}

func (f *factorizationImpl) Do(
	done <-chan struct{},
	numbers []int,
	writer io.Writer,
	config ...Config,
) error {
	panic("not implemented")
}
