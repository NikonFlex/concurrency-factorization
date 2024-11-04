# Factorization

## Задание
В данном домашнем задании вам необходимо реализовать следующий интерфейс

```go
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
```

Примеры использования можно посмотреть в [тестах](/internal/fact/fact_test.go)

С одним из алгоритмов можно ознакомиться по [ссылке](https://ru.wikipedia.org/wiki/Перебор_делителей)

## Пример:

#### Input:

`[100, -17, 25, 38], 3`

#### Output:
```text
100 = 2 * 2 * 5 * 5
-17 = -1 * 17
38 = 2 * 19
25 = 5 * 5
```

## Особенности реализации

- Множители записываются от меньшего к большему
- Если число меньше нуля, добавляется множитель -1
- Используйте тесты, чтобы заполнить недосказанности.

## Сдача

* Все функции реализовать в файле [fact.go](/internal/fact/fact.go)
* Открыть pull request из ветки `hw` в ветку `main` **вашего репозитория**.
* В описании PR заполнить количество часов, которые вы потратили на это задание.
* Отправить заявку на ревью в соответствующей форме.
* Время дедлайна фиксируется отправкой формы.
* Изменять файлы в ветке main без PR запрещено.

## Makefile

Для удобств локальной разработки сделан [`Makefile`](Makefile). Имеются следующие команды:

Запустить полный цикл (линтер, тесты):

```bash 
make all
```

Запустить только тесты:

```bash
make test
``` 

Запустить линтер:

```bash
make lint
```

Подтянуть новые тесты:

```bash
make update
```

При разработке на Windows рекомендуется использовать [WSL](https://learn.microsoft.com/en-us/windows/wsl/install), чтобы
была возможность пользоваться вспомогательными скриптами.
