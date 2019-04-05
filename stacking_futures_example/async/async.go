package async

import (
	"sync"
	"time"
)

// Value represents the value returned by a query
type Value struct {
	Result interface{}
	Err    error
}

type waiter struct {
	timeSet time.Time
	value   *Value
	wg      sync.WaitGroup
}

func (w *waiter) write(value *Value) {
	w.value = value
	w.wg.Done()
}

// Future represents a futures object.
type Future struct {
	waiter *waiter
}

// NewFuture returns a new future with the provided waiter.
func NewFuture(w *waiter) Future {
	if w == nil {
		panic(w)
	}
	return Future{
		waiter: w,
	}
}

// BlockForValue blocks until the value is available.
func (f *Future) BlockForValue() *Value {
	f.waiter.wg.Wait()
	return f.waiter.value
}

// GeneratorFunc takes in a unique set of params and generates a result and error.
type GeneratorFunc func(interface{}) (interface{}, error)

// Cache manages futures.
type Cache struct {
	gf      GeneratorFunc
	timeout time.Duration
	waiters map[interface{}]*waiter
	lock    sync.Mutex
}

// NewCache returns a futures cache.
func NewCache(gf GeneratorFunc, timeout time.Duration) *Cache {
	return &Cache{
		gf:      gf,
		timeout: timeout,
		waiters: make(map[interface{}]*waiter),
	}
}

// Get gets futures that return values corresponding to the params.
func (c *Cache) Get(params []interface{}) []Future {
	c.lock.Lock()
	defer c.lock.Unlock()

	futures := make([]Future, len(params))
	for i, param := range params {
		w, ok := c.waiters[param]
		// Only invalidate if it doesnt exist or if the waiter has timed out.
		// If the current waiter is still processing, its ok. The objects requested
		// before will get old values, but thats ok.
		if !ok || time.Since(w.timeSet) > c.timeout {
			w = new(waiter)
			w.wg.Add(1)
			c.waiters[param] = w
			w.timeSet = time.Now()

			// Spawn async load function.
			// TODO: add support to batch these maybe (for performance reasons)?
			go c.loadWaiter(w, param)
		}
		futures[i] = Future{
			waiter: w,
		}
	}
	return futures
}

func (c *Cache) loadWaiter(w *waiter, param interface{}) {
	res, err := c.gf(param)
	w.write(&Value{
		Result: res,
		Err:    err,
	})
}
