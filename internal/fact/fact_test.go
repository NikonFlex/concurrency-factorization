package fact

import (
	"bufio"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

var _ Factorization = (*factorizationImpl)(nil)

func TestNoInternalState(t *testing.T) {
	require.EqualValues(t, 0, unsafe.Sizeof(factorizationImpl{}))
}

type testCancel struct {
	name         string
	factWorkers  int
	writeWorkers int
	numbers      []int
	doneSleep    time.Duration
	beforeSleep  time.Duration
	writer       io.Writer
	err          require.ErrorAssertionFunc
}

func runCancel(t *testing.T, tt *testCancel) {
	done := make(chan struct{})

	fact := New()
	config := Config{
		FactorizationWorkers: tt.factWorkers,
		WriteWorkers:         tt.writeWorkers,
	}

	wg := new(sync.WaitGroup)
	if tt.doneSleep != -1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(tt.doneSleep)
			close(done)
		}()
	}

	time.Sleep(tt.beforeSleep)
	err := fact.Do(done, tt.numbers, tt.writer, config)
	wg.Wait()

	tt.err(t, err)
}

func TestWorkersCount(t *testing.T) {
	done := make(chan struct{})

	fact := New()
	numbers := getNumbers(100)
	config := Config{
		FactorizationWorkers: 16,
		WriteWorkers:         16,
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second)
		err := fact.Do(done, numbers, newSleepWriter(time.Second*5), config)
		require.NoError(t, err)
	}()

	var workers int
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 100)
		workers = max(workers, runtime.NumGoroutine())
	}

	wg.Wait()
	require.LessOrEqual(t, workers, 36)
	require.LessOrEqual(t, runtime.NumGoroutine(), 3)
}

func TestMixedCancel(t *testing.T) {
	runCancel(t, &testCancel{
		name:         "input",
		factWorkers:  1000,
		writeWorkers: 1000,
		numbers:      getNumbers(1_000_000),
		doneSleep:    time.Millisecond * 100,
		beforeSleep:  0,
		err: func(t require.TestingT, err error, i ...interface{}) {
			require.True(t, errors.Is(err, ErrWriterInteraction) || errors.Is(err, ErrFactorizationCancelled))
		},
		writer: newSleepErrorWriter(time.Millisecond*100, errors.New("123")),
	})
}

func TestWriterHasInternalErrorCancel(t *testing.T) {
	var errWriteInternalFail = errors.New("writer internally failed" + strconv.FormatInt(rand.N[int64](10e9), 10))

	runCancel(t, &testCancel{
		name:         "input",
		factWorkers:  100,
		writeWorkers: 100000,
		numbers:      getNumbers(10_000_000),
		doneSleep:    -1,
		beforeSleep:  0,
		err: func(t require.TestingT, err error, i ...interface{}) {
			require.ErrorIs(t, err, errWriteInternalFail)
		},
		writer: newSleepErrorWriter(time.Millisecond*100, errWriteInternalFail),
	})
}

func TestWriterErrorCancel(t *testing.T) {
	runCancel(t, &testCancel{
		name:         "input",
		factWorkers:  100,
		writeWorkers: 100000,
		numbers:      getNumbers(10_000_000),
		doneSleep:    -1,
		beforeSleep:  0,
		err: func(t require.TestingT, err error, i ...interface{}) {
			require.ErrorIs(t, err, ErrWriterInteraction)
		},
		writer: newSleepErrorWriter(time.Millisecond*100, errors.New("123")),
	})
}

func TestCancelInput(t *testing.T) {
	runCancel(t, &testCancel{
		name:         "input",
		factWorkers:  1,
		writeWorkers: 1,
		numbers:      []int{1},
		doneSleep:    0,
		beforeSleep:  3 * time.Second,
		err: func(t require.TestingT, err error, i ...interface{}) {
			require.ErrorIs(t, err, ErrFactorizationCancelled)
		},
		writer: newSleepWriter(time.Second * 5),
	})
}

func TestCancelLow(t *testing.T) {
	runCancel(t, &testCancel{
		name:         "low",
		factWorkers:  5,
		writeWorkers: 1,
		numbers:      getNumbers(10),
		doneSleep:    time.Second * 3,
		err: func(t require.TestingT, err error, i ...interface{}) {
			require.ErrorIs(t, err, ErrFactorizationCancelled)
		},
		writer: newSleepWriter(time.Second * 5),
	})
}

func TestInvalidConfig(t *testing.T) {
	t.Run("fact workers", func(t *testing.T) {
		done := make(chan struct{})

		fact := New()
		numbers := getNumbers(10)
		config := Config{
			FactorizationWorkers: -1,
			WriteWorkers:         1,
		}

		err := fact.Do(done, numbers, newWriter(), config)
		require.Error(t, err)
	})

	t.Run("write workers", func(t *testing.T) {
		done := make(chan struct{})

		fact := New()
		numbers := getNumbers(10)
		config := Config{
			FactorizationWorkers: 1,
			WriteWorkers:         -1,
		}

		err := fact.Do(done, numbers, newWriter(), config)
		require.Error(t, err)
	})
}

func TestFallbackPerformance(t *testing.T) {
	first := testing.Benchmark(func(b *testing.B) {
		done := make(chan struct{})

		fact := New()
		numbers := getNumbers(10)
		config := Config{
			FactorizationWorkers: runtime.GOMAXPROCS(0),
			WriteWorkers:         runtime.GOMAXPROCS(0),
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := fact.Do(done, numbers, newWriter(), config)
			require.NoError(t, err)
		}
	})

	second := testing.Benchmark(func(b *testing.B) {
		done := make(chan struct{})

		fact := New()
		numbers := getNumbers(10)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := fact.Do(done, numbers, newWriter())
			require.NoError(t, err)
		}
	})

	require.LessOrEqual(t, float64(second.NsPerOp())/float64(first.NsPerOp()), 1.4)
}

func TestNoGoroutineLeak(t *testing.T) {
	done := make(chan struct{})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 1)
		close(done)
	}()

	fact := New()
	numbers := getNumbers(1_000_000)
	config := Config{
		FactorizationWorkers: 1000,
		WriteWorkers:         1,
	}

	err := fact.Do(done, numbers, newSleepWriter(time.Millisecond), config)
	require.Error(t, err)
	require.LessOrEqual(t, runtime.NumGoroutine(), 3)
}

func TestGeneralPerformance(t *testing.T) {
	first := testing.Benchmark(func(b *testing.B) {
		done := make(chan struct{})

		fact := New()
		numbers := getNumbers(100)
		config := Config{
			FactorizationWorkers: max(2, runtime.NumCPU()),
			WriteWorkers:         max(4, runtime.NumCPU()),
		}

		for i := 0; i < b.N; i++ {
			err := fact.Do(done, numbers, newSleepWriter(time.Millisecond*100), config)
			require.NoError(t, err)
		}
	})

	second := testing.Benchmark(func(b *testing.B) {
		done := make(chan struct{})

		fact := New()
		numbers := getNumbers(100)
		config := Config{
			FactorizationWorkers: max(2, runtime.NumCPU()) / 2,
			WriteWorkers:         max(4, runtime.NumCPU()) / 4,
		}

		for i := 0; i < b.N; i++ {
			err := fact.Do(done, numbers, newSleepWriter(time.Millisecond*100), config)
			require.NoError(t, err)
		}
	})

	require.GreaterOrEqual(t, float64(second.NsPerOp())/float64(first.NsPerOp()), 3.5)
}

func getNumbers(n int) []int {
	s := make([]int, 0, n)

	for i := 0; i < n; i++ {
		s = append(s, i)
	}

	return s
}

func TestGoldenOutput(t *testing.T) {
	testCases := []struct {
		name    string
		numbers []int
		config  Config
		want    []string
	}{
		{
			name:    "positive",
			numbers: []int{1, 2, 3, 4, 5},
			config: Config{
				FactorizationWorkers: 2,
				WriteWorkers:         2,
			},
			want: []string{
				"1 = 1",
				"2 = 2",
				"3 = 3",
				"4 = 2 * 2",
				"5 = 5",
			},
		},
		{
			name:    "negative",
			numbers: []int{0, 100, -17, 25, 38},
			config: Config{
				FactorizationWorkers: 1,
				WriteWorkers:         1,
			},
			want: []string{
				"0 = 0",
				"100 = 2 * 2 * 5 * 5",
				"-17 = -1 * 17",
				"25 = 5 * 5",
				"38 = 2 * 19",
			},
		},
		{
			name:    "big",
			numbers: []int{1073741824, 4, 4},
			config: Config{
				FactorizationWorkers: 100,
				WriteWorkers:         1100,
			},
			want: []string{
				"4 = 2 * 2",
				"4 = 2 * 2",
				"1073741824 = 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2",
			},
		},
		{
			name:    "simple",
			numbers: []int{10, 4, 4, 12, 15, 27, 33, 19, 14, -5, -10, -20},
			config: Config{
				FactorizationWorkers: 5,
				WriteWorkers:         5,
			},
			want: []string{
				"10 = 2 * 5",
				"4 = 2 * 2",
				"4 = 2 * 2",
				"12 = 2 * 2 * 3",
				"15 = 3 * 5",
				"27 = 3 * 3 * 3",
				"33 = 3 * 11",
				"19 = 19",
				"14 = 2 * 7",
				"-5 = -1 * 5",
				"-10 = -1 * 2 * 5",
				"-20 = -1 * 2 * 2 * 5",
			},
		},
		{
			name:    "empty",
			numbers: []int{},
			config: Config{
				FactorizationWorkers: 1,
				WriteWorkers:         1,
			},
			want: []string{},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			fact := New()
			writer := newWriter()
			done := make(chan struct{})
			err := fact.Do(done, tt.numbers, writer, tt.config)
			require.NoError(t, err)

			facts := getFact(writer)
			slices.Sort(tt.want)
			slices.Sort(facts)

			require.Equal(t, tt.want, facts)
		})
	}
}

type TestFactorizationCorrectness struct {
	factWorkers  int
	writeWorkers int
	input        []int
}

func getDone() <-chan struct{} {
	return make(chan struct{})
}

func (tc TestFactorizationCorrectness) Run(t *testing.T) {
	factorization := New()

	cfg := Config{
		FactorizationWorkers: tc.factWorkers,
		WriteWorkers:         tc.writeWorkers,
	}

	writer := newWriter()
	done := getDone()
	err := factorization.Do(done, tc.input, writer, cfg)
	require.NoError(t, err)
	scanner := bufio.NewScanner(strings.NewReader(writer.sb.String()))

	allNums := make([]int, 0, len(tc.input))
	for scanner.Scan() {
		line := scanner.Text()
		num, res := parseLine(t, line)
		require.True(t, checkFactorization(num, res))
		allNums = append(allNums, num)
	}

	require.NoError(t, scanner.Err())

	sort.Ints(allNums)
	sort.Ints(tc.input)

	require.Equal(t, tc.input, allNums)
}

func TestCorrectness(t *testing.T) {
	TestFactorizationCorrectness{
		factWorkers:  1,
		writeWorkers: 1,
		input:        []int{math.MinInt},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  1,
		writeWorkers: 1,
		input:        []int{math.MinInt + 1},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  1,
		writeWorkers: 1,
		input:        []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, math.MaxInt32 - 13},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  1,
		writeWorkers: 5,
		input:        []int{-10, -225, -100, 250, 9, 7, 5, 3, -117, -1},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  2,
		writeWorkers: 2,
		input:        []int{12, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  3,
		writeWorkers: 1,
		input:        []int{100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  4,
		writeWorkers: 3,
		input:        []int{-3, -6, -9, -12, -15, -18, -21, -24, -27, -30},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  2,
		writeWorkers: 4,
		input:        []int{200, 225, 250, 275, 300, 325, 350, 375, 400, 425, 450},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  5,
		writeWorkers: 5,
		input:        []int{500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  3,
		writeWorkers: 6,
		input:        []int{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  2,
		writeWorkers: 2,
		input:        []int{-50, -60, -70, -80, -90, -100, -110, -120, -130, -140},
	}.Run(t)

	TestFactorizationCorrectness{
		factWorkers:  4,
		writeWorkers: 4,
		input:        []int{144, 169, 196, 225, 256, 289, 324, 361, 400, 441, 484, 529},
	}.Run(t)
}

func getFact(writer TestWriter) []string {
	r := strings.TrimRight(writer.String(), "\n")

	if r == "" {
		return []string{}
	}

	return strings.Split(r, "\n")
}

type TestWriter interface {
	io.Writer
	String() string
}

type concurrentWriter struct {
	sb *strings.Builder
	mx *sync.RWMutex
}

type sleepErrorWriter struct {
	sb        *strings.Builder
	mx        *sync.RWMutex
	sleepTime time.Duration
	err       error
}

func newSleepErrorWriter(sleepTime time.Duration, err error) *sleepErrorWriter {
	return &sleepErrorWriter{
		sb:        new(strings.Builder),
		mx:        new(sync.RWMutex),
		sleepTime: sleepTime,
		err:       err,
	}
}

func (s *sleepErrorWriter) Write(p []byte) (n int, err error) {
	time.Sleep(s.sleepTime)

	s.mx.Lock()
	defer s.mx.Unlock()

	return 0, s.err
}

type sleepWriter struct {
	sb        *strings.Builder
	mx        *sync.RWMutex
	sleepTime time.Duration
}

func newSleepWriter(sleepTime time.Duration) *sleepWriter {
	return &sleepWriter{
		sb:        new(strings.Builder),
		mx:        new(sync.RWMutex),
		sleepTime: sleepTime,
	}
}

func (s *sleepWriter) Write(p []byte) (n int, err error) {
	time.Sleep(s.sleepTime)

	s.mx.Lock()
	defer s.mx.Unlock()

	return s.sb.Write(p)
}

func newWriter() *concurrentWriter {
	return &concurrentWriter{
		sb: new(strings.Builder),
		mx: new(sync.RWMutex),
	}
}

func (c *concurrentWriter) Write(p []byte) (n int, err error) {
	c.mx.Lock()
	defer c.mx.Unlock()

	return c.sb.Write(p)
}

func (c *concurrentWriter) String() string {
	c.mx.RLock()
	defer c.mx.RUnlock()

	return c.sb.String()
}

func strToInt(t *testing.T, str string) int {
	t.Helper()

	d, err := strconv.Atoi(strings.TrimSpace(str))
	require.NoError(t, err)

	return d
}

func delimiterStringsToSliceInt(t *testing.T, delimiterStrings []string) []int {
	t.Helper()

	delimiters := make([]int, 0, len(delimiterStrings))
	for _, dString := range delimiterStrings {
		num := strToInt(t, dString)
		delimiters = append(delimiters, num)
	}

	return delimiters
}

func parseLine(t *testing.T, line string) (int, []int) {
	t.Helper()

	s := strings.Split(line, "=")
	left := strToInt(t, s[0])
	right := delimiterStringsToSliceInt(t, strings.Split(strings.ReplaceAll(strings.Join(s[1:], ""),
		" ", ""), "*"))

	return left, right
}

func checkFactorization(num int, delimiters []int) bool {
	if !slices.IsSortedFunc(delimiters, func(i, j int) int {
		return i - j
	}) {
		return false
	}

	got := 1
	for _, d := range delimiters {
		if !pChecker.IsPrime(int(math.Abs(float64(d)))) {
			return false
		}

		got *= d
	}

	return num == got
}

var pChecker = newPrimeChecker()

type primeChecker struct {
	memo map[int]bool
}

func newPrimeChecker() *primeChecker {
	return &primeChecker{
		memo: make(map[int]bool),
	}
}

func (pc *primeChecker) IsPrime(n int) bool {
	if n == 1 || n == 2 {
		return true
	}

	if result, exists := pc.memo[n]; exists {
		return result
	}

	sqrtN := int(math.Sqrt(float64(n)))
	isPrime := true
	for i := 2; i <= sqrtN; i++ {
		if n%i == 0 {
			isPrime = false
			break
		}
	}

	pc.memo[n] = isPrime

	return isPrime
}
