package main

import (
	"factorization/internal/fact"
	"fmt"
	"log"
	"math"
	"os"
)

func main() {
	slice := []int{math.MaxInt32 - 13}
	done := make(chan struct{})
	if err := fact.New().Do(done, slice, os.Stdout, fact.Config{
		FactorizationWorkers: 2,
		WriteWorkers:         2,
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Finished")
}
