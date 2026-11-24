package keyorder_test

import (
	"fmt"
	"strings"
	"sync"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/lni/dragonboat/v4/statemachine/keyorder"
)

// ExampleAccountSM_Update shows the account balance use case: two changes of
// the same account are committed in log order and applied by the key ordered
// state machine. The batch [set 200, add -50] ends at 150.
func ExampleAccountSM_Update() {
	stateMachine := keyorder.NewAccountSM()
	account := "acct-1"
	entries := []sm.Entry{
		{
			Index: 1,
			Cmd:   keyorder.Command{Account: account, Op: keyorder.OpSet, Amount: 200}.Encode(),
		},
		{
			Index: 2,
			Cmd:   keyorder.Command{Account: account, Op: keyorder.OpAdd, Amount: -50}.Encode(),
		},
	}
	if _, err := stateMachine.Update(entries); err != nil {
		panic(err)
	}
	value, err := stateMachine.Lookup(account)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(value.([]byte)))
	// Output: 150
}

// ExampleApply shows that entries of one key stay in batch order while the
// helper is free to run unrelated keys concurrently.
func ExampleApply() {
	batch := []string{"a:1", "b:1", "a:2", "a:3", "b:2"}
	var mu sync.Mutex
	seen := make(map[string][]string)
	err := keyorder.Apply(batch,
		func(item string) string { return strings.Split(item, ":")[0] },
		func(item string) error {
			key := strings.Split(item, ":")[0]
			mu.Lock()
			seen[key] = append(seen[key], item)
			mu.Unlock()
			return nil
		})
	if err != nil {
		panic(err)
	}
	fmt.Println(seen["a"])
	fmt.Println(seen["b"])
	// Output:
	// [a:1 a:2 a:3]
	// [b:1 b:2]
}
