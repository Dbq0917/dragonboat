package keyorder

import (
	sm "github.com/lni/dragonboat/v4/statemachine"
)

// Update applies one committed entry batch with per key ordering.
//
// keyOf extracts the affected key from an entry command. apply executes the
// actual state machine update for the entry and returns its result. Entries
// that touch the same key are applied serially in their batch order, entries
// of different keys may be applied concurrently. The Result field of every
// handled entry is set from the returned sm.Result. The input entries slice
// is returned as required by sm.IConcurrentStateMachine.Update.
//
// Commands are parsed for their keys before any update runs. When keyOf
// returns an error for any command, no entry is applied and the error is
// returned. When apply fails for an entry, ordering follows the semantics of
// Executor.ApplyIndex.
func Update(entries []sm.Entry,
	keyOf func(cmd []byte) (string, error),
	apply func(entry *sm.Entry) (sm.Result, error)) ([]sm.Entry, error) {
	return NewExecutor(0).Update(entries, keyOf, apply)
}

// Update is the Executor bound variant of the package level Update helper.
func (e *Executor) Update(entries []sm.Entry,
	keyOf func(cmd []byte) (string, error),
	apply func(entry *sm.Entry) (sm.Result, error)) ([]sm.Entry, error) {
	keys := make([]string, len(entries))
	for i := range entries {
		key, err := keyOf(entries[i].Cmd)
		if err != nil {
			return entries, err
		}
		keys[i] = key
	}
	err := e.ApplyIndex(len(entries),
		func(i int) string { return keys[i] },
		func(i int) error {
			result, err := apply(&entries[i])
			if err != nil {
				return err
			}
			entries[i].Result = result
			return nil
		})
	return entries, err
}

// NewExecutor creates an Executor with the specified maximum number of key
// strands allowed to run concurrently. A maxConcurrent value <= 0 removes
// the limit, every distinct key gets its own strand.
func NewExecutor(maxConcurrent int) *Executor {
	return &Executor{MaxConcurrent: maxConcurrent}
}
