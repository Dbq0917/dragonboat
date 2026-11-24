package keyorder

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestApplyIndexEmptyBatch(t *testing.T) {
	called := false
	if err := ApplyIndex(0,
		func(int) string { return "" },
		func(int) error { called = true; return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Fatal("apply must not be called for an empty batch")
	}
	if err := Apply(nil,
		func(any) string { return "" },
		func(any) error { return nil }); err != nil {
		t.Fatalf("unexpected generic apply error: %v", err)
	}
}

func TestApplyPreservesOrderAndExclusivityForSameKey(t *testing.T) {
	const keys = 8
	const n = keys * 40
	for round := 0; round < 25; round++ {
		orders := make([][]int, keys)
		var active [keys]atomic.Int32
		var overlap atomic.Bool
		err := ApplyIndex(n,
			func(i int) string { return fmt.Sprintf("k%d", i%keys) },
			func(i int) error {
				k := i % keys
				if active[k].Add(1) > 1 {
					overlap.Store(true)
				}
				if i%37 == 0 {
					time.Sleep(20 * time.Microsecond)
				}
				orders[k] = append(orders[k], i)
				active[k].Add(-1)
				return nil
			})
		if err != nil {
			t.Fatalf("round %d: unexpected error: %v", round, err)
		}
		if overlap.Load() {
			t.Fatalf("round %d: entries of the same key were applied concurrently", round)
		}
		for k := 0; k < keys; k++ {
			want := make([]int, 0, n/keys)
			for i := k; i < n; i += keys {
				want = append(want, i)
			}
			if len(orders[k]) != len(want) {
				t.Fatalf("round %d key %d: got %d entries, want %d",
					round, k, len(orders[k]), len(want))
			}
			for j := range want {
				if orders[k][j] != want[j] {
					t.Fatalf("round %d key %d: order mismatch at %d: got %d want %d (full=%v)",
						round, k, j, orders[k][j], want[j], orders[k])
				}
			}
		}
	}
}

func TestApplyRunsDifferentKeysConcurrently(t *testing.T) {
	started := make(chan int, 2)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- ApplyIndex(2,
			func(i int) string { return fmt.Sprintf("independent-key-%d", i) },
			func(i int) error {
				started <- i
				<-release
				return nil
			})
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(15 * time.Second):
			close(release)
			t.Fatal("different keys were not applied concurrently")
		}
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ApplyIndex did not return after releasing strands")
	}
}

func TestApplyRunsManyDifferentKeysConcurrently(t *testing.T) {
	const n = 64
	var current atomic.Int32
	var peak atomic.Int32
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- ApplyIndex(n,
			func(i int) string { return fmt.Sprintf("key-%d", i) },
			func(i int) error {
				cur := current.Add(1)
				for {
					p := peak.Load()
					if cur <= p || peak.CompareAndSwap(p, cur) {
						break
					}
				}
				<-release
				current.Add(-1)
				return nil
			})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for current.Load() != n {
		if time.Now().After(deadline) {
			close(release)
			t.Fatalf("only %d of %d distinct keys ran concurrently", current.Load(), n)
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ApplyIndex did not return after releasing strands")
	}
	if peak.Load() != n {
		t.Fatalf("peak concurrency = %d, want %d", peak.Load(), n)
	}
}

func TestExecutorConcurrencyCapIsRespected(t *testing.T) {
	const cap = 4
	const n = 80
	e := NewExecutor(cap)
	var current atomic.Int32
	var peak atomic.Int32
	var processed atomic.Int32
	err := e.ApplyIndex(n,
		func(i int) string { return fmt.Sprintf("k-%d", i) },
		func(i int) error {
			cur := current.Add(1)
			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}
			time.Sleep(50 * time.Microsecond)
			processed.Add(1)
			current.Add(-1)
			return nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed.Load() != n {
		t.Fatalf("processed %d, want %d", processed.Load(), n)
	}
	if peak.Load() > cap {
		t.Fatalf("peak concurrency %d exceeds cap %d", peak.Load(), cap)
	}
}

func TestApplyStopsFailedStrandButKeepsOtherKeys(t *testing.T) {
	errSentinel := errors.New("boom")
	var mu sync.Mutex
	called := make([]int, 0, 4)
	err := ApplyIndex(4,
		func(i int) string {
			if i == 3 {
				return "other"
			}
			return "hot"
		},
		func(i int) error {
			mu.Lock()
			called = append(called, i)
			mu.Unlock()
			if i == 1 {
				return errSentinel
			}
			return nil
		})
	if !errors.Is(err, errSentinel) {
		t.Fatalf("expected joined error to contain %v, got %v", errSentinel, err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := map[int]bool{0: true, 1: true, 3: true}
	if len(called) != len(want) {
		t.Fatalf("called indices %v, want 0,1,3 (index 2 must be skipped after failure)", called)
	}
	for _, i := range called {
		if !want[i] {
			t.Fatalf("unexpected called index %d in %v", i, called)
		}
	}
}

func TestApplyGenericPreservesOrder(t *testing.T) {
	type update struct {
		key string
		seq int
	}
	batch := make([]update, 0, 20)
	for i := 0; i < 20; i++ {
		key := "a"
		if i%2 == 0 {
			key = "b"
		}
		batch = append(batch, update{key: key, seq: i})
	}
	var mu sync.Mutex
	got := map[string][]int{}
	if err := Apply(batch,
		func(u update) string { return u.key },
		func(u update) error {
			mu.Lock()
			got[u.key] = append(got[u.key], u.seq)
			mu.Unlock()
			return nil
		}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, key := range []string{"a", "b"} {
		for j := 1; j < len(got[key]); j++ {
			if got[key][j-1] >= got[key][j] {
				t.Fatalf("key %s order not preserved: %v", key, got[key])
			}
		}
	}
}
