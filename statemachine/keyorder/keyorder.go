// Package keyorder applies one ordered batch of replicated log entries so
// that entries updating the same key are applied serially in log order,
// while entries of different keys are still allowed to run concurrently.
//
// A Raft log batch is committed in a strict total order. Fanning every entry
// of such a batch out to a worker goroutine concurrently can let two entries
// that modify the same key race each other: both read the old value, both
// compute a new value and the late writer overwrites the early one. The
// resulting state no longer matches the committed log and replaying the log
// after a restart can reach a different result.
//
// Apply and ApplyIndex group the batch by key without changing its order.
// Every key gets its own serial strand that handles that key's entries in
// their original batch order, independent keys run on independent strands.
// The functions block until the whole batch has been handled, so callers
// (typically an IConcurrentStateMachine.Update implementation) can safely
// return the filled entry results afterwards.
package keyorder

import (
	"errors"
	"sync"
)

// Executor applies ordered batches with key affine ordering.
//
// The zero value Executor applies every distinct key on its own goroutine
// without imposing a global concurrency limit, so unrelated keys are never
// queued behind each other. Set MaxConcurrent to a positive value to bound
// the number of key strands that may run at the same time; this is back
// pressure only, entries of the same key always stay on one ordered strand.
type Executor struct {
	// MaxConcurrent is the maximum number of key strands allowed to run at
	// once. A value <= 0 means no limit.
	MaxConcurrent int
}

// ApplyIndex fans n ordered batch items out by key. keyOf maps the item index
// to its key and apply handles the item at index i.
//
// Indices sharing a key are passed to apply serially in ascending order (the
// order in which they appear in the batch). Indices mapped to different keys
// may be applied concurrently. ApplyIndex returns after all strands have
// finished.
//
// Once apply fails for an item, the strand of that key stops handling the
// remaining items of the same key: a failed update must not be followed by
// updates that might depend on it. Other keys are not affected. The first
// error of every failed strand is joined into the returned error.
func (e *Executor) ApplyIndex(n int,
	keyOf func(i int) string, apply func(i int) error) error {
	if n <= 0 {
		return nil
	}
	if keyOf == nil || apply == nil {
		panic("keyorder: keyOf and apply must not be nil")
	}
	strands := make(map[string][]int)
	keys := make([]string, 0)
	for i := 0; i < n; i++ {
		key := keyOf(i)
		if _, ok := strands[key]; !ok {
			keys = append(keys, key)
		}
		strands[key] = append(strands[key], i)
	}
	sem := e.semaphore()
	var wg sync.WaitGroup
	errs := make([]error, len(keys))
	for strandID, key := range keys {
		indices := strands[key]
		wg.Add(1)
		go func(id int, indices []int) {
			defer wg.Done()
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}
			for _, idx := range indices {
				if err := apply(idx); err != nil {
					errs[id] = err
					return
				}
			}
		}(strandID, indices)
	}
	wg.Wait()
	return joinErrors(errs)
}

func (e *Executor) semaphore() chan struct{} {
	if e == nil || e.MaxConcurrent <= 0 {
		return nil
	}
	return make(chan struct{}, e.MaxConcurrent)
}

// ApplyIndex is the package level shorthand for an unbounded Executor.
func ApplyIndex(n int,
	keyOf func(i int) string, apply func(i int) error) error {
	return (&Executor{}).ApplyIndex(n, keyOf, apply)
}

// Apply fans the typed ordered batch out by key. Items sharing a key are
// applied serially in batch order, different keys may run concurrently.
func Apply[T any](items []T,
	keyOf func(item T) string, apply func(item T) error) error {
	return ApplyIndex(len(items),
		func(i int) string { return keyOf(items[i]) },
		func(i int) error { return apply(items[i]) })
}

func joinErrors(errs []error) error {
	filtered := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return errors.Join(filtered...)
}
