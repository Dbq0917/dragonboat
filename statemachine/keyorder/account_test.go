package keyorder

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

func entry(index uint64, c Command) sm.Entry {
	return sm.Entry{Index: index, Cmd: c.Encode()}
}

// TestAccountBalanceTwoConcurrentMessagesSameKey is the application use case
// from the task: two balance change messages for the same account are
// submitted concurrently and the final balance is observed. Both deposits
// must be retained, a last writer wins implementation would lose one.
func TestAccountBalanceTwoConcurrentMessagesSameKey(t *testing.T) {
	for round := 0; round < 100; round++ {
		s := NewAccountSM()
		account := fmt.Sprintf("acct-concurrent-%d", round)
		messages := []Command{
			{Account: account, Op: OpAdd, Amount: 100},
			{Account: account, Op: OpAdd, Amount: 50},
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, len(messages))
		for i, msg := range messages {
			wg.Add(1)
			go func(index uint64, msg Command) {
				defer wg.Done()
				<-start
				_, err := s.Update([]sm.Entry{entry(index, msg)})
				errs <- err
			}(uint64(i+1), msg)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("unexpected update error: %v", err)
			}
		}
		balance, ok := s.Balance(account)
		if !ok {
			t.Fatalf("round %d: account %s missing", round, account)
		}
		if balance != 150 {
			t.Fatalf("round %d: balance = %d, want 150 (one update was overwritten)",
				round, balance)
		}
	}
	t.Log("100 concurrent same-account submissions all settled at balance=150")
}

// TestAccountBalanceBatchUsesCommittedOrder proves that the batch order is
// the log order and that reversing the two messages really changes the
// balance. The committed order [set 200, add -50] must end at 150.
func TestAccountBalanceBatchUsesCommittedOrder(t *testing.T) {
	const account = "acct-order"
	for round := 0; round < 100; round++ {
		s := NewAccountSM()
		committed := []sm.Entry{
			entry(1, Command{Account: account, Op: OpSet, Amount: 200}),
			entry(2, Command{Account: account, Op: OpAdd, Amount: -50}),
		}
		result, err := s.Update(committed)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if len(result) != 2 {
			t.Fatalf("round %d: got %d results", round, len(result))
		}
		balance, _ := s.Balance(account)
		if balance != 150 {
			t.Fatalf("round %d: committed order balance = %d, want 150", round, balance)
		}

		reversed := NewAccountSM()
		if _, err := reversed.Update([]sm.Entry{
			entry(1, Command{Account: account, Op: OpAdd, Amount: -50}),
			entry(2, Command{Account: account, Op: OpSet, Amount: 200}),
		}); err != nil {
			t.Fatalf("round %d reversed: %v", round, err)
		}
		reversedBalance, _ := reversed.Balance(account)
		if reversedBalance != 200 {
			t.Fatalf("round %d: reversed order balance = %d, want 200",
				round, reversedBalance)
		}
		if balance == reversedBalance {
			t.Fatal("test setup is order insensitive, it cannot prove ordering")
		}
		if round == 0 {
			t.Logf("final value: committed order [set 200, add -50] => %d; reversed => %d",
				balance, reversedBalance)
		}
	}
}

// TestAccountBalanceBatchDoesNotLoseDeposits applies two deposits in one
// ordered batch through the keyorder helper.
func TestAccountBalanceBatchDoesNotLoseDeposits(t *testing.T) {
	const account = "acct-deposits"
	for round := 0; round < 100; round++ {
		s := NewAccountSM()
		_, err := s.Update([]sm.Entry{
			entry(1, Command{Account: account, Op: OpAdd, Amount: 100}),
			entry(2, Command{Account: account, Op: OpAdd, Amount: 50}),
		})
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if balance, _ := s.Balance(account); balance != 150 {
			t.Fatalf("round %d: balance = %d, want 150", round, balance)
		}
	}
}

// TestAccountStateMatchesLogReplayAfterRestart proposes two order sensitive
// messages from concurrent goroutines, commits them in the observed log
// order and then replays the exact same log on a fresh state machine. State,
// log derived expectation and restarted replay must all agree.
func TestAccountStateMatchesLogReplayAfterRestart(t *testing.T) {
	for round := 0; round < 50; round++ {
		account := fmt.Sprintf("acct-replay-%d", round)
		messages := []Command{
			{Account: account, Op: OpSet, Amount: 200},
			{Account: account, Op: OpAdd, Amount: -50},
		}
		var logMu sync.Mutex
		committedLog := make([][]byte, 0, 2)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, msg := range messages {
			wg.Add(1)
			go func(msg Command) {
				defer wg.Done()
				<-start
				logMu.Lock()
				committedLog = append(committedLog, msg.Encode())
				logMu.Unlock()
			}(msg)
		}
		close(start)
		wg.Wait()

		entries := make([]sm.Entry, 0, len(committedLog))
		expected := int64(0)
		for i, cmd := range committedLog {
			c, err := DecodeCommand(cmd)
			if err != nil {
				t.Fatal(err)
			}
			switch c.Op {
			case OpAdd:
				expected += c.Amount
			case OpSet:
				expected = c.Amount
			}
			entries = append(entries, entry(uint64(i+1), c))
		}

		s := NewAccountSM()
		if _, err := s.Update(entries); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		stateBalance, _ := s.Balance(account)
		if stateBalance != expected {
			t.Fatalf("round %d: state %d does not match log derived %d",
				round, stateBalance, expected)
		}

		restarted := NewAccountSM()
		replay := make([]sm.Entry, len(entries))
		copy(replay, entries)
		if _, err := restarted.Update(replay); err != nil {
			t.Fatalf("round %d replay: %v", round, err)
		}
		replayedBalance, _ := restarted.Balance(account)
		if replayedBalance != expected {
			t.Fatalf("round %d: replay %d differs from log derived %d",
				round, replayedBalance, expected)
		}
		if replayedBalance != stateBalance {
			t.Fatalf("round %d: replay %d differs from live state %d",
				round, replayedBalance, stateBalance)
		}
	}
}

func TestAccountLookupAndSnapshotRoundTrip(t *testing.T) {
	s := NewAccountSM()
	accounts := []string{"a", "b", "c"}
	batch := make([]sm.Entry, 0)
	idx := uint64(1)
	for _, name := range accounts {
		batch = append(batch,
			entry(idx, Command{Account: name, Op: OpSet, Amount: 100}))
		idx++
		batch = append(batch,
			entry(idx, Command{Account: name, Op: OpAdd, Amount: 25}))
		idx++
	}
	if _, err := s.Update(batch); err != nil {
		t.Fatal(err)
	}
	for _, name := range accounts {
		v, err := s.Lookup([]byte(name))
		if err != nil {
			t.Fatal(err)
		}
		if string(v.([]byte)) != "125" {
			t.Fatalf("Lookup(%s) = %q, want 125", name, v)
		}
	}

	ctx, err := s.PrepareSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := s.SaveSnapshot(ctx, &buf, nil, make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	restored := NewAccountSM()
	if err := restored.RecoverFromSnapshot(bytes.NewReader(buf.Bytes()),
		nil, make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	for _, name := range accounts {
		want, _ := s.Balance(name)
		got, _ := restored.Balance(name)
		if got != want {
			t.Fatalf("restored %s = %d, want %d", name, got, want)
		}
	}
}

func TestAccountConcurrentUpdatesAndLookupsWithRace(t *testing.T) {
	s := NewAccountSM()
	const accounts = 8
	const updatesPerAccount = 120
	const readers = 8
	var wg sync.WaitGroup
	for a := 0; a < accounts; a++ {
		name := fmt.Sprintf("race-acct-%d", a)
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]sm.Entry, 0, updatesPerAccount)
			for i := 0; i < updatesPerAccount; i++ {
				batch = append(batch,
					entry(uint64(i+1), Command{Account: name, Op: OpAdd, Amount: 1}))
			}
			if _, err := s.Update(batch); err != nil {
				t.Errorf("update %s: %v", name, err)
			}
		}()
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				name := fmt.Sprintf("race-acct-%d", (id+i)%accounts)
				if _, err := s.Lookup(name); err != nil {
					t.Errorf("lookup: %v", err)
					return
				}
				_ = s.Balances()
			}
		}(r)
	}
	wg.Wait()
	for a := 0; a < accounts; a++ {
		name := fmt.Sprintf("race-acct-%d", a)
		if balance, _ := s.Balance(name); balance != updatesPerAccount {
			t.Fatalf("%s balance = %d, want %d", name, balance, updatesPerAccount)
		}
	}
}

func TestUpdateAdapterRejectsUnparsableCommandWithoutApplying(t *testing.T) {
	applied := false
	_, err := Update([]sm.Entry{{Cmd: []byte("bad")}},
		func(cmd []byte) (string, error) {
			return "", fmt.Errorf("cannot parse %q", cmd)
		},
		func(entry *sm.Entry) (sm.Result, error) {
			applied = true
			return sm.Result{}, nil
		})
	if err == nil {
		t.Fatal("expected parse error")
	}
	if applied {
		t.Fatal("apply must not run when key extraction fails")
	}
}
