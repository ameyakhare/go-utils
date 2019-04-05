package main

import (
	"fmt"
	"perf_tests/async"
	"sync"
	"sync/atomic"
	"time"
)

type value struct {
	str     string
	timeSet time.Time
}

const (
	evictionDuration = 100 * time.Millisecond
	readDuration     = 5 * time.Millisecond
	numRequests      = 1000000
)

var counter uint64

func generateValue(params interface{}) (interface{}, error) {

	atomic.AddUint64(&counter, 1)
	// Simulate a redis read under load
	time.Sleep(readDuration)
	return &value{
		str:     "apple",
		timeSet: time.Now(),
	}, nil
}

func main() {
	runOldImplementationTest()
	runNewImplementationTest()
}

func runOldImplementationTest() {
	counter = 0
	values := make(map[interface{}]*value)
	lock := sync.Mutex{}
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go simulateOldImplementation(&lock, values, &wg)
	}
	wg.Wait()
	fmt.Println("Old Implementation Results:")
	fmt.Printf("Took %v\n", time.Since(start))
	fmt.Printf("Read %d times\n", counter)
}

func simulateOldImplementation(lock *sync.Mutex,
	values map[interface{}]*value, wg *sync.WaitGroup) {

	defer wg.Done()
	key := "hello"
	now := time.Now()
	lock.Lock()
	v, ok := values[key]
	if !ok || now.Sub(v.timeSet) > evictionDuration {
		delete(values, key)
	} else {
		lock.Unlock()
		return
	}
	lock.Unlock()

	vint, _ := generateValue(key)

	lock.Lock()
	v = vint.(*value)
	v.timeSet = now
	values[key] = v
	lock.Unlock()
}

func runNewImplementationTest() {
	counter = 0
	start := time.Now()
	wg := sync.WaitGroup{}
	m := async.NewCache(generateValue, evictionDuration)
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go simulateNewImplementation(m, &wg)
	}
	wg.Wait()
	fmt.Println("New Implementation Results:")
	fmt.Printf("Took %v\n", time.Since(start))
	fmt.Printf("Read %d times\n", counter)
}

func simulateNewImplementation(m *async.Cache, wg *sync.WaitGroup) {
	objects := m.Get([]interface{}{"hello"})
	objects[0].BlockForValue()
	wg.Done()
}
