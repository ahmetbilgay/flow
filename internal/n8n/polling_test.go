package n8n

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/dgmos/flow/internal/models"
)

func pollTestCtx(store PollStateStore) *execCtx {
	wf := &models.Workflow{ID: "wf-poll", Nodes: []models.Node{{ID: "trig", Name: "Trigger"}}}
	data := newExecData(&models.Execution{ID: "e"}, wf, nil, &Options{})
	return &execCtx{ctx: context.Background(), opts: &Options{PollState: store}, node: &wf.Nodes[0], data: data}
}

// A poll that read the cursor before another instance advanced it must not
// emit its items again.
func TestPollCommitLosesToConcurrentPoll(t *testing.T) {
	store := &memPollState{m: map[string]string{}}
	ec := pollTestCtx(store)
	key := ec.pollKey()
	_ = store.SetPollState(ec.ctx, key, "100")

	pc, err := ec.openPoll()
	if err != nil {
		t.Fatal(err)
	}
	_ = store.SetPollState(ec.ctx, key, "150") // another instance polled meanwhile
	out, err := pc.commit("200", []Item{{JSON: map[string]interface{}{"id": "m1"}}})
	pc.close()
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || len(out[0]) != 0 {
		t.Fatalf("losing poll emitted %v, want no items", out)
	}
	if v, _, _ := store.GetPollState(ec.ctx, key); v != "150" {
		t.Fatalf("cursor = %q, want the winner's 150", v)
	}

	// The first poll of a trigger must also lose if another created the cursor.
	delete(store.m, key)
	pc, _ = ec.openPoll()
	_ = store.SetPollState(ec.ctx, key, "999")
	out, _ = pc.commit("1", []Item{{JSON: map[string]interface{}{}}})
	pc.close()
	if len(out[0]) != 0 {
		t.Fatal("initial poll overwrote a concurrently created cursor")
	}
}

type plainPollStore struct{ s *memPollState } // no CompareAndSwap

func (p plainPollStore) GetPollState(ctx context.Context, k string) (string, bool, error) {
	return p.s.GetPollState(ctx, k)
}
func (p plainPollStore) SetPollState(ctx context.Context, k, v string) error {
	return p.s.SetPollState(ctx, k, v)
}

// Without CAS, overlapping polls in one process are serialized: no poll
// reads a cursor another poll is about to advance.
func TestPollSerializedInProcess(t *testing.T) {
	store := plainPollStore{&memPollState{m: map[string]string{}}}
	ec := pollTestCtx(store)
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pc, err := ec.openPoll()
			if err != nil {
				t.Error(err)
				return
			}
			defer pc.close()
			cur, _ := strconv.Atoi(pc.prev)
			if _, err := pc.commit(strconv.Itoa(cur+1), nil); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if v, _, _ := store.GetPollState(ec.ctx, ec.pollKey()); v != strconv.Itoa(n) {
		t.Fatalf("cursor = %s after %d polls; overlapping polls read the same cursor", v, n)
	}
}
