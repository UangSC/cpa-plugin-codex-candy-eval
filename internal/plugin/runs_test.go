package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// Blocking runners let tests control completion without contacting an upstream or relying on sleeps.
type controlledRuns struct {
	started chan string
	finish  map[string]chan struct{}
}

func newControlledRuns(ids ...string) *controlledRuns {
	c := &controlledRuns{started: make(chan string, len(ids)), finish: map[string]chan struct{}{}}
	for _, id := range ids {
		c.finish[id] = make(chan struct{})
	}
	return c
}

func (c *controlledRuns) submit(t *testing.T, test string, ids ...string) runSummary {
	t.Helper()
	var auths []credential
	for _, id := range ids {
		auths = append(auths, credential{ID: id, Provider: "codex"})
	}
	response := startCredentialRuns(test, runRequest{Model: "m"}, auths, 1, func(string) {}, func(ctx context.Context, auth credential, _ *progress) {
		c.started <- auth.ID
		select {
		case <-ctx.Done():
		case <-c.finish[auth.ID]:
		}
	})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("submit: %s", response.Body)
	}
	return mustDecode[runSummary](response.Body)
}

func (c *controlledRuns) next(t *testing.T) string {
	t.Helper()
	select {
	case id := <-c.started:
		return id
	case <-time.After(3 * time.Second):
		t.Fatal("queued credential did not start")
		return ""
	}
}

func checkScheduler(t *testing.T, active, queued int) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if got := schedulerStatusLocked(); got.Active != active || got.Queued != queued {
		t.Fatalf("scheduler = %+v, want active=%d queued=%d", got, active, queued)
	}
}

func waitScheduler(t *testing.T, active, queued int) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		mu.Lock()
		got := schedulerStatusLocked()
		mu.Unlock()
		if got.Active == active && got.Queued == queued {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("scheduler did not settle: %+v, want active=%d queued=%d", got, active, queued)
		}
	}
}

func setConcurrency(t *testing.T, limit int) {
	t.Helper()
	if r := settingsResponse([]byte(fmt.Sprintf(`{"credential_concurrency":%d}`, limit))); r.StatusCode != 200 {
		t.Fatalf("settings: %s", r.Body)
	}
}

func TestCredentialQueueFIFOAndSharedLimit(t *testing.T) {
	setupTest(t)
	c := newControlledRuns("a", "b", "c", "d", "e")
	if s := c.submit(t, "candy", "a", "b", "c"); s.Started != 3 || s.Queued != 1 {
		t.Fatalf("first batch: %+v", s)
	}
	first := []string{c.next(t), c.next(t)}
	slices.Sort(first)
	if !slices.Equal(first, []string{"a", "b"}) {
		t.Fatalf("first runners: %v", first)
	}
	if s := c.submit(t, "pelican", "d"); s.Queued != 1 {
		t.Fatalf("second test bypassed shared limit: %+v", s)
	}
	c.submit(t, "fingerprint", "e")
	if s := c.submit(t, "candy", "c"); s.Started != 0 || s.Busy != 1 || s.Queued != 0 {
		t.Fatalf("duplicate queued credential: %+v", s)
	}
	checkScheduler(t, 2, 3)
	close(c.finish["b"])
	if got := c.next(t); got != "c" {
		t.Fatalf("completion must immediately start oldest queued credential: %s", got)
	}
	close(c.finish["a"])
	if got := c.next(t); got != "d" {
		t.Fatalf("FIFO across tests: %s", got)
	}
	close(c.finish["c"])
	if got := c.next(t); got != "e" {
		t.Fatalf("FIFO across tests: %s", got)
	}
	close(c.finish["d"])
	close(c.finish["e"])
	tasks.Wait()
	checkScheduler(t, 0, 0)
}

func TestCredentialQueueCancellation(t *testing.T) {
	setupTest(t)
	setConcurrency(t, 1)
	c := newControlledRuns("a", "b", "c", "d")
	c.submit(t, "candy", "a", "b", "c", "d")
	c.next(t)
	if r := cancelResponse([]byte(`{"credential_ids":["b","c"]}`)); string(r.Body) != `{"cancelled":2}` {
		t.Fatalf("cancel queued: %s", r.Body)
	}
	checkScheduler(t, 1, 1)
	mu.Lock()
	removed := running["b"] == nil && running["c"] == nil
	mu.Unlock()
	if !removed {
		t.Fatal("cancelled queued credentials still marked busy")
	}
	cancelResponse([]byte(`{"credential_ids":["a"]}`))
	if got := c.next(t); got != "d" {
		t.Fatalf("cancelled queue item ran: %s", got)
	}
	close(c.finish["d"])
	tasks.Wait()
	checkScheduler(t, 0, 0)
}

func TestCredentialQueueLimitChanges(t *testing.T) {
	setupTest(t)
	setConcurrency(t, 1)
	c := newControlledRuns("a", "b", "c", "d")
	c.submit(t, "candy", "a", "b", "c", "d")
	c.next(t)
	setConcurrency(t, 2)
	if got := c.next(t); got != "b" {
		t.Fatalf("increasing limit did not fill slot: %s", got)
	}
	setConcurrency(t, 1)
	checkScheduler(t, 2, 2)
	// A completes while B is still active: C must remain queued after a decrease to one.
	close(c.finish["a"])
	waitScheduler(t, 1, 2)
	mu.Lock()
	cancelled := running["b"].Cancelling
	mu.Unlock()
	if cancelled {
		t.Fatal("decreasing the limit cancelled active work")
	}
	close(c.finish["b"])
	if got := c.next(t); got != "c" {
		t.Fatalf("next runner: %s", got)
	}
	checkScheduler(t, 1, 1)
	close(c.finish["c"])
	if got := c.next(t); got != "d" {
		t.Fatalf("next runner: %s", got)
	}
	close(c.finish["d"])
	tasks.Wait()
}

func TestCredentialQueueQuiesce(t *testing.T) {
	setupTest(t)
	c := newControlledRuns("a", "b", "c", "d")
	c.submit(t, "candy", "a", "b", "c", "d")
	c.next(t)
	c.next(t)
	Quiesce()
	checkScheduler(t, 0, 0)
	if len(running) != 0 || len(c.started) != 0 {
		t.Fatal("quiesce started queued work or leaked tasks")
	}
	if r := settingsResponse([]byte(`{"credential_concurrency":3}`)); r.StatusCode != 503 {
		t.Fatal("quiescing plugin accepted settings")
	}
	HandleMethod("plugin.reconfigure", nil)
	c.submit(t, "siwc", "c")
	if got := c.next(t); got != "c" {
		t.Fatalf("reconfigured scheduler: %s", got)
	}
	close(c.finish["c"])
	tasks.Wait()
}

func TestCredentialConcurrencyPersistenceAndValidation(t *testing.T) {
	setupTest(t)
	setConcurrency(t, 3)
	records.Candies.add("keep", candyRecord{record: record{Status: "correct"}})
	if err := saveStateLocked(); err != nil {
		t.Fatal(err)
	}
	records, loaded = emptyState(), false
	loadState()
	if records.CredentialConcurrency != 3 || len(records.Candies["keep"]) != 1 {
		t.Fatal("setting or history lost on reload")
	}
	if r := clearRecordsResponse("all"); r.StatusCode != 200 || records.CredentialConcurrency != 3 {
		t.Fatal("clearing history reset concurrency")
	}
	for _, body := range []string{`{`, `{}`, `{"credential_concurrency":0}`, `{"credential_concurrency":11}`, `{"credential_concurrency":1.5}`, `{"credential_concurrency":"2"}`} {
		if r := settingsResponse([]byte(body)); r.StatusCode != 400 || records.CredentialConcurrency != 3 {
			t.Fatalf("invalid setting changed limit: %s", body)
		}
	}
	statePath = filepath.Join(t.TempDir(), "missing", "state.json")
	if r := settingsResponse([]byte(`{"credential_concurrency":4}`)); r.StatusCode != 500 || records.CredentialConcurrency != 3 {
		t.Fatal("failed save changed live concurrency")
	}
	for _, file := range []string{`{"schema_version":3}`, `{"schema_version":3,"credential_concurrency":0}`, `{"schema_version":3,"credential_concurrency":11}`} {
		s, _, err := readState([]byte(file))
		if err != nil || s.CredentialConcurrency != 2 {
			t.Fatalf("legacy default: %+v %v", s, err)
		}
	}
	// The setting is additive; it doesn't force a schema migration or discard an older backup.
	statePath, loaded = filepath.Join(t.TempDir(), "legacy.json"), false
	if err := os.WriteFile(statePath, []byte(`{"schema_version":3,"candies":{"keep":[{"status":"correct"}]}}`), 0600); err != nil {
		t.Fatal(err)
	}
	loadState()
	if records.CredentialConcurrency != 2 || len(records.Candies["keep"]) != 1 {
		t.Fatal("legacy state was not preserved")
	}
}

func TestCandyFailureFreesCredentialSlot(t *testing.T) {
	for _, answer := range []string{"failed", "retry", "wrong"} {
		t.Run(answer, func(t *testing.T) {
			setupTest(t)
			setConcurrency(t, 1)
			var calls []string
			hostCall = streamHost(func(method string, payload any) (json.RawMessage, error) {
				if method == "host.auth.list" {
					return json.RawMessage(`{"files":[{"id":"a","provider":"codex"},{"id":"b","provider":"codex"}]}`), nil
				}
				raw, _ := json.Marshal(payload)
				var req struct {
					ID string `json:"auth_id"`
				}
				_ = json.Unmarshal(raw, &req)
				calls = append(calls, req.ID)
				if req.ID == "a" && answer == "retry" {
					return nil, &EnvelopeError{Code: "unavailable", Message: "upstream unavailable", HTTPStatus: 503}
				}
				if req.ID == "a" && answer == "failed" {
					return nil, &EnvelopeError{Code: "quota", Message: "quota exhausted", HTTPStatus: 429}
				}
				return mockModelResponse("20"), nil
			})
			candyRunResponse([]byte(`{"all":true,"model":"m","runs":3}`))
			tasks.Wait()
			want := []string{"a", "b", "b", "b"}
			if answer == "wrong" || answer == "retry" {
				want = []string{"a", "a", "a", "b", "b", "b"}
			}
			if !slices.Equal(calls, want) {
				t.Fatalf("request sequence = %v, want %v", calls, want)
			}
			status := answer
			if answer == "retry" {
				status = "failed"
			}
			if records.Candies["a"][0].Status != status {
				t.Fatalf("result = %+v", records.Candies["a"])
			}
		})
	}
}
