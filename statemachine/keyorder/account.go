package keyorder

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// Balance operation names carried by Command.
const (
	// OpAdd adds Amount to the current balance, Amount may be negative.
	OpAdd = "add"
	// OpSet resets the balance to Amount.
	OpSet = "set"
)

var errAccountSMClosed = errors.New("keyorder: account state machine closed")

// Command is one account balance change. It is the text encoded command used
// by AccountSM, the wire format is:
//
//	"<account> <add|set> <amount>"
//
// for example "acct-7 add 100" or "acct-7 set 250".
type Command struct {
	// Account is the key that ordering is preserved for.
	Account string
	// Op is either OpAdd or OpSet.
	Op string
	// Amount is the delta for OpAdd or the new balance for OpSet.
	Amount int64
}

// Encode returns the deterministic text encoding of the command.
func (c Command) Encode() []byte {
	return []byte(fmt.Sprintf("%s %s %d", c.Account, c.Op, c.Amount))
}

// DecodeCommand parses a command produced by Command.Encode.
func DecodeCommand(cmd []byte) (Command, error) {
	fields := strings.Fields(string(cmd))
	if len(fields) != 3 {
		return Command{}, fmt.Errorf("keyorder: invalid account command %q", cmd)
	}
	amount, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return Command{}, fmt.Errorf("keyorder: invalid amount in %q: %w", cmd, err)
	}
	op := fields[1]
	if op != OpAdd && op != OpSet {
		return Command{}, fmt.Errorf("keyorder: unknown operation %q", op)
	}
	return Command{Account: fields[0], Op: op, Amount: amount}, nil
}

type account struct {
	mu      sync.Mutex
	balance int64
}

// AccountSM is an in memory account balance state machine.
//
// Its Update method applies a committed batch through the keyorder helper:
// balance changes of the same account are applied serially in committed log
// order, changes of different accounts can run concurrently. A per account
// mutex additionally makes the read modify write balance update atomic even
// when Update is invoked concurrently by tests.
//
// AccountSM is safe for concurrent use.
type AccountSM struct {
	mu       sync.RWMutex
	accounts map[string]*account
	closed   atomic.Bool
}

var _ sm.IConcurrentStateMachine = (*AccountSM)(nil)

// NewAccountSM creates an empty AccountSM.
func NewAccountSM() *AccountSM {
	return &AccountSM{accounts: make(map[string]*account)}
}

// Update applies the committed entry batch with per account ordering.
func (s *AccountSM) Update(entries []sm.Entry) ([]sm.Entry, error) {
	if s.closed.Load() {
		return entries, errAccountSMClosed
	}
	return Update(entries,
		func(cmd []byte) (string, error) {
			c, err := DecodeCommand(cmd)
			return c.Account, err
		},
		func(entry *sm.Entry) (sm.Result, error) {
			c, err := DecodeCommand(entry.Cmd)
			if err != nil {
				return sm.Result{}, err
			}
			acc := s.account(c.Account)
			acc.mu.Lock()
			switch c.Op {
			case OpAdd:
				acc.balance += c.Amount
			case OpSet:
				acc.balance = c.Amount
			}
			balance := acc.balance
			acc.mu.Unlock()
			return sm.Result{Data: []byte(strconv.FormatInt(balance, 10))}, nil
		})
}

// Lookup returns the decimal balance of the account named by query. query can
// be a string or a []byte. Unknown accounts have a zero balance.
func (s *AccountSM) Lookup(query interface{}) (interface{}, error) {
	if s.closed.Load() {
		return nil, errAccountSMClosed
	}
	var name string
	switch q := query.(type) {
	case string:
		name = q
	case []byte:
		name = string(q)
	default:
		return nil, fmt.Errorf("keyorder: unsupported lookup key %T", query)
	}
	balance, _ := s.Balance(name)
	return []byte(strconv.FormatInt(balance, 10)), nil
}

// Balance returns the current balance of the named account. The boolean
// reports whether the account has ever been updated.
func (s *AccountSM) Balance(name string) (int64, bool) {
	s.mu.RLock()
	acc := s.accounts[name]
	s.mu.RUnlock()
	if acc == nil {
		return 0, false
	}
	acc.mu.Lock()
	balance := acc.balance
	acc.mu.Unlock()
	return balance, true
}

// Balances returns a point in time copy of all known balances, it is mainly
// used by tests and snapshots.
func (s *AccountSM) Balances() map[string]int64 {
	s.mu.RLock()
	names := make([]string, 0, len(s.accounts))
	accs := make([]*account, 0, len(s.accounts))
	for name, acc := range s.accounts {
		names = append(names, name)
		accs = append(accs, acc)
	}
	s.mu.RUnlock()
	out := make(map[string]int64, len(names))
	for i, acc := range accs {
		acc.mu.Lock()
		out[names[i]] = acc.balance
		acc.mu.Unlock()
	}
	return out
}

// PrepareSnapshot captures the current balances as a JSON ready map.
func (s *AccountSM) PrepareSnapshot() (interface{}, error) {
	return s.Balances(), nil
}

// SaveSnapshot writes the prepared balance map as JSON.
func (s *AccountSM) SaveSnapshot(ctx interface{},
	w io.Writer, _ sm.ISnapshotFileCollection,
	done <-chan struct{}) error {
	if s.closed.Load() {
		return errAccountSMClosed
	}
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
	}
	balances, ok := ctx.(map[string]int64)
	if !ok {
		return fmt.Errorf("keyorder: unexpected snapshot context %T", ctx)
	}
	data, err := json.Marshal(balances)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// RecoverFromSnapshot restores balances from the JSON written by SaveSnapshot.
func (s *AccountSM) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, done <-chan struct{}) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	balances := make(map[string]int64)
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := json.Unmarshal(data, &balances); err != nil {
			return err
		}
	}
	next := make(map[string]*account, len(balances))
	for name, balance := range balances {
		next[name] = &account{balance: balance}
	}
	s.mu.Lock()
	s.accounts = next
	s.mu.Unlock()
	return nil
}

// Close marks the state machine closed, it does not remove the balances.
func (s *AccountSM) Close() error {
	s.closed.Store(true)
	return nil
}

func (s *AccountSM) account(name string) *account {
	s.mu.RLock()
	acc := s.accounts[name]
	s.mu.RUnlock()
	if acc != nil {
		return acc
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if acc = s.accounts[name]; acc != nil {
		return acc
	}
	acc = &account{}
	s.accounts[name] = acc
	return acc
}
