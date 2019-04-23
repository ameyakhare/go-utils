package main

import (
	"fmt"
	"perf_tests/stream"
	"time"
)

var total int

func finish(raw interface{}) {
	total = total + 1
}

func main() {
	m := stream.NewSequencedStore(10000000, finish)

	now := time.Now()
	for i := 0; i < 10000000; i++ {
		msg := m.GetSequencedMessage(i)
		msg.Done()
	}
	fmt.Printf("Took %s\n", time.Since(now).String())
}
