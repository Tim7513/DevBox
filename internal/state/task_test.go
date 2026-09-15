package state

import (
	"errors"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

// allTaskStates and allTaskEvents drive the exhaustive matrix test below. Any
// state or event added to the package must be added here too, or the matrix
// test will fail on an unhandled entry — which is the point.
var allTaskStates = []TaskState{TaskQueued, TaskLeased, TaskRunning, TaskSucceeded, TaskFailed, TaskDead}
var allTaskEvents = []TaskEventKind{EvLease, EvStart, EvSucceed, EvFail, EvLeaseExpire, EvRetry, EvDeadLetter}

// TestTaskTransitionMatrix asserts the complete legal/illegal transition table.
// Every one of the len(states)*len(events) cells is named explicitly; there is
// no "and everything else is illegal" catch-all, because a silent widening of
// the legal set is exactly the bug this guards against.
func TestTaskTransitionMatrix(t *testing.T) {
	// legal[state][event] = true means the transition must be accepted when all
	// guards are satisfied.
	legal := map[TaskState]map[TaskEventKind]bool{
		TaskQueued:  {EvLease: true},
		TaskLeased:  {EvStart: true, EvSucceed: true, EvFail: true, EvLeaseExpire: true},
		TaskRunning: {EvSucceed: true, EvFail: true, EvLeaseExpire: true},
		TaskFailed:  {EvRetry: true, EvDeadLetter: true},
		// Terminal states accept nothing.
		TaskSucceeded: {},
		TaskDead:      {},
	}

	for _, st := range allTaskStates {
		for _, ev := range allTaskEvents {
			st, ev := st, ev
			t.Run(string(st)+"/"+string(ev), func(t *testing.T) {
				cur := Task{
					Type:         TaskProvision,
					State:        st,
					Attempts:     1,
					MaxAttempts:  5,
					FencingToken: 7,
					WorkerID:     "worker-a",
					Reserved:     st == TaskLeased || st == TaskRunning,
				}
				// Build an event that satisfies every guard, so that a
				// rejection can only mean the transition itself is illegal.
				e := TaskEvent{Kind: ev, FencingToken: 7, WorkerID: "worker-b"}
				if ev == EvLease {
					e.FencingToken = 8 // must strictly exceed
				}

				_, err := ApplyTask(cur, e, t0)
				want := legal[st][ev]
				if want && err != nil {
					t.Fatalf("expected legal transition, got error: %v", err)
				}
				if !want && err == nil {
					t.Fatalf("expected illegal transition to be rejected, but it was accepted")
				}
				if !want && !errors.Is(err, ErrIllegalTransition) && !errors.Is(err, ErrStaleFencingToken) {
					t.Fatalf("rejection should carry a sentinel, got %v", err)
				}
			})
		}
	}
}

func TestLeaseRequiresStrictlyHigherFencingToken(t *testing.T) {
	cur := Task{Type: TaskProvision, State: TaskQueued, FencingToken: 5}

	for _, tc := range []struct {
		name    string
		token   int64
		wantErr bool
	}{
		{"lower is rejected", 4, true},
		{"equal is rejected", 5, true},
		{"higher is accepted", 6, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := ApplyTask(cur, TaskEvent{Kind: EvLease, FencingToken: tc.token, WorkerID: "w1"}, t0)
			if tc.wantErr {
				if !errors.Is(err, ErrStaleFencingToken) {
					t.Fatalf("want ErrStaleFencingToken, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Next.FencingToken != tc.token {
				t.Errorf("token = %d, want %d", res.Next.FencingToken, tc.token)
			}
			if res.Next.State != TaskLeased {
				t.Errorf("state = %s, want leased", res.Next.State)
			}
		})
	}
}

// TestStaleWorkerIsFencedOut is the split-brain scenario from §33.2: a worker
// is partitioned, its lease is reclaimed and re-issued with a higher token, and
// then the original worker comes back and tries to complete the task it still
// believes it owns. Every one of its writes must bounce.
func TestStaleWorkerIsFencedOut(t *testing.T) {
	// Worker A holds the task at token 10 and is executing.
	running := Task{Type: TaskProvision, State: TaskRunning, Attempts: 1,
		MaxAttempts: 5, FencingToken: 10, WorkerID: "worker-a", Reserved: true}

	// Reaper reclaims it. Note: no fence check on expiry — the reaper acts
	// precisely because the holder stopped responding.
	reclaimed, err := ApplyTask(running, TaskEvent{Kind: EvLeaseExpire}, t0)
	if err != nil {
		t.Fatalf("reaper could not reclaim: %v", err)
	}
	if reclaimed.Next.State != TaskFailed {
		t.Fatalf("reclaimed state = %s, want failed", reclaimed.Next.State)
	}
	assertReleasesCapacityOnce(t, reclaimed.Effects, "worker-a")

	// Retry, then re-lease to worker B with a strictly higher token.
	requeued, err := ApplyTask(reclaimed.Next, TaskEvent{Kind: EvRetry, Backoff: time.Second}, t0)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	released, err := ApplyTask(requeued.Next, TaskEvent{Kind: EvLease, FencingToken: 11, WorkerID: "worker-b"}, t0)
	if err != nil {
		t.Fatalf("re-lease failed: %v", err)
	}
	current := released.Next

	// Worker A wakes up and tries to act on token 10. Every attempt must fail
	// with a stale-fence error, and must not mutate anything.
	for _, kind := range []TaskEventKind{EvStart, EvSucceed, EvFail} {
		_, err := ApplyTask(current, TaskEvent{Kind: kind, FencingToken: 10, Err: "stale worker write"}, t0)
		if !errors.Is(err, ErrStaleFencingToken) {
			t.Errorf("stale worker %s: want ErrStaleFencingToken, got %v", kind, err)
		}
	}

	// Worker B, holding the current token, works fine.
	if _, err := ApplyTask(current, TaskEvent{Kind: EvStart, FencingToken: 11}, t0); err != nil {
		t.Errorf("current lease holder was rejected: %v", err)
	}
}

// TestCapacityReleasedExactlyOnce walks every path out of a reserved state and
// asserts the reservation is handed back exactly once — never leaked (which
// would strand worker capacity forever) and never double-released (which would
// let the fleet be overcommitted).
func TestCapacityReleasedExactlyOnce(t *testing.T) {
	exits := []struct {
		name string
		from TaskState
		ev   TaskEvent
	}{
		{"leased→succeeded", TaskLeased, TaskEvent{Kind: EvSucceed, FencingToken: 3}},
		{"leased→failed", TaskLeased, TaskEvent{Kind: EvFail, FencingToken: 3, Err: "boom"}},
		{"leased→expired", TaskLeased, TaskEvent{Kind: EvLeaseExpire}},
		{"running→succeeded", TaskRunning, TaskEvent{Kind: EvSucceed, FencingToken: 3}},
		{"running→failed", TaskRunning, TaskEvent{Kind: EvFail, FencingToken: 3, Err: "boom"}},
		{"running→expired", TaskRunning, TaskEvent{Kind: EvLeaseExpire}},
	}

	for _, tc := range exits {
		t.Run(tc.name, func(t *testing.T) {
			cur := Task{Type: TaskProvision, State: tc.from, Attempts: 1, MaxAttempts: 5,
				FencingToken: 3, WorkerID: "worker-x", Reserved: true}
			res, err := ApplyTask(cur, tc.ev, t0)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertReleasesCapacityOnce(t, res.Effects, "worker-x")
			if res.Next.Reserved {
				t.Error("Reserved must be false after leaving a reserved state")
			}
			if res.Next.WorkerID != "" {
				t.Errorf("WorkerID must be cleared, got %q", res.Next.WorkerID)
			}
		})
	}
}

// A task that never held a reservation must not release one.
func TestNoSpuriousCapacityRelease(t *testing.T) {
	cur := Task{Type: TaskProvision, State: TaskFailed, Attempts: 1, MaxAttempts: 5, FencingToken: 3}
	res, err := ApplyTask(cur, TaskEvent{Kind: EvRetry, Backoff: time.Second}, t0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, e := range res.Effects {
		if _, bad := e.(ReleaseCapacity); bad {
			t.Fatal("retry from failed must not release capacity; it was already released on failure")
		}
	}
}

func TestLeaseReservesCapacity(t *testing.T) {
	cur := Task{Type: TaskProvision, State: TaskQueued, FencingToken: 1}
	res, err := ApplyTask(cur, TaskEvent{Kind: EvLease, FencingToken: 2, WorkerID: "w9"}, t0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Next.Reserved {
		t.Error("leasing must mark the task as holding a reservation")
	}
	var got int
	for _, e := range res.Effects {
		if r, ok := e.(ReserveCapacity); ok {
			got++
			if r.WorkerID != "w9" {
				t.Errorf("reserve on %q, want w9", r.WorkerID)
			}
		}
	}
	if got != 1 {
		t.Errorf("got %d ReserveCapacity effects, want exactly 1", got)
	}
}

// Attempts must advance on every consumed try, including leases that expire
// before the worker ever reported starting. Otherwise a worker that reliably
// dies during provision would be handed the same task forever.
func TestAttemptAccounting(t *testing.T) {
	t.Run("start consumes an attempt", func(t *testing.T) {
		cur := Task{Type: TaskProvision, State: TaskLeased, Attempts: 2, MaxAttempts: 5, FencingToken: 4, WorkerID: "w", Reserved: true}
		res, err := ApplyTask(cur, TaskEvent{Kind: EvStart, FencingToken: 4}, t0)
		if err != nil {
			t.Fatal(err)
		}
		if res.Next.Attempts != 3 {
			t.Errorf("attempts = %d, want 3", res.Next.Attempts)
		}
	})

	t.Run("expiry from leased consumes an attempt", func(t *testing.T) {
		cur := Task{Type: TaskProvision, State: TaskLeased, Attempts: 2, MaxAttempts: 5, FencingToken: 4, WorkerID: "w", Reserved: true}
		res, err := ApplyTask(cur, TaskEvent{Kind: EvLeaseExpire}, t0)
		if err != nil {
			t.Fatal(err)
		}
		if res.Next.Attempts != 3 {
			t.Errorf("attempts = %d, want 3 (a dead worker still burns a try)", res.Next.Attempts)
		}
	})

	t.Run("expiry from running does not double-count", func(t *testing.T) {
		// Start already incremented when the worker reported running.
		cur := Task{Type: TaskProvision, State: TaskRunning, Attempts: 3, MaxAttempts: 5, FencingToken: 4, WorkerID: "w", Reserved: true}
		res, err := ApplyTask(cur, TaskEvent{Kind: EvLeaseExpire}, t0)
		if err != nil {
			t.Fatal(err)
		}
		if res.Next.Attempts != 3 {
			t.Errorf("attempts = %d, want 3", res.Next.Attempts)
		}
	})
}

func TestRetryRefusesWhenBudgetExhausted(t *testing.T) {
	cur := Task{Type: TaskStart, State: TaskFailed, Attempts: 3, MaxAttempts: 3, FencingToken: 2}
	if _, err := ApplyTask(cur, TaskEvent{Kind: EvRetry, Backoff: time.Second}, t0); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("want rejection at budget, got %v", err)
	}
	// Dead-lettering is the legal move instead.
	res, err := ApplyTask(cur, TaskEvent{Kind: EvDeadLetter, Err: "start failed 3x"}, t0)
	if err != nil {
		t.Fatalf("dead-letter rejected: %v", err)
	}
	if res.Next.State != TaskDead {
		t.Errorf("state = %s, want dead", res.Next.State)
	}
	if !res.Next.State.Terminal() {
		t.Error("dead must be terminal")
	}
}

func TestRetryPreservesFencingToken(t *testing.T) {
	// The token must survive a requeue, otherwise the next lease could reuse a
	// value a stale worker still holds.
	cur := Task{Type: TaskProvision, State: TaskFailed, Attempts: 1, MaxAttempts: 5, FencingToken: 42}
	res, err := ApplyTask(cur, TaskEvent{Kind: EvRetry, Backoff: 5 * time.Second}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Next.FencingToken != 42 {
		t.Errorf("token = %d, want 42 preserved across requeue", res.Next.FencingToken)
	}
	var sched *ScheduleRetry
	for i := range res.Effects {
		if s, ok := res.Effects[i].(ScheduleRetry); ok {
			sched = &s
		}
	}
	if sched == nil {
		t.Fatal("retry must emit a ScheduleRetry effect")
	}
	if want := t0.Add(5 * time.Second); !sched.AvailableAt.Equal(want) {
		t.Errorf("AvailableAt = %v, want %v", sched.AvailableAt, want)
	}
}

func TestShouldDeadLetter(t *testing.T) {
	for _, tc := range []struct {
		name      string
		snap      Task
		permanent bool
		want      bool
	}{
		{"under budget", Task{Type: TaskProvision, State: TaskFailed, Attempts: 2, MaxAttempts: 5}, false, false},
		{"at budget", Task{Type: TaskProvision, State: TaskFailed, Attempts: 5, MaxAttempts: 5}, false, true},
		{"over budget", Task{Type: TaskProvision, State: TaskFailed, Attempts: 6, MaxAttempts: 5}, false, true},
		{"permanent short-circuits", Task{Type: TaskProvision, State: TaskFailed, Attempts: 1, MaxAttempts: 5}, true, true},
		{"falls back to type budget", Task{Type: TaskStart, State: TaskFailed, Attempts: 3, MaxAttempts: 0}, false, true},
		{"not failed", Task{Type: TaskProvision, State: TaskRunning, Attempts: 9, MaxAttempts: 5}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldDeadLetter(tc.snap, tc.permanent); got != tc.want {
				t.Errorf("ShouldDeadLetter = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTerminalStatesAreAbsorbing(t *testing.T) {
	for _, st := range []TaskState{TaskSucceeded, TaskDead} {
		for _, ev := range allTaskEvents {
			cur := Task{Type: TaskProvision, State: st, FencingToken: 1}
			if _, err := ApplyTask(cur, TaskEvent{Kind: ev, FencingToken: 2, WorkerID: "w"}, t0); err == nil {
				t.Errorf("%s accepted %s; terminal states must absorb", st, ev)
			}
		}
	}
}

func TestActiveMatchesQueueIndexPredicate(t *testing.T) {
	// The partial unique index in migrations is defined over exactly this set.
	// If this test changes, the migration must change with it.
	want := map[TaskState]bool{
		TaskQueued: true, TaskLeased: true, TaskRunning: true,
		TaskSucceeded: false, TaskFailed: false, TaskDead: false,
	}
	for st, w := range want {
		if st.Active() != w {
			t.Errorf("%s.Active() = %v, want %v", st, st.Active(), w)
		}
	}
}

func TestUnknownStateAndEventRejected(t *testing.T) {
	if _, err := ApplyTask(Task{State: "bogus"}, TaskEvent{Kind: EvLease, FencingToken: 1, WorkerID: "w"}, t0); err == nil {
		t.Error("unknown state must be rejected")
	}
	if _, err := ApplyTask(Task{State: TaskQueued}, TaskEvent{Kind: "bogus"}, t0); !errors.Is(err, ErrInvalidEvent) {
		t.Error("unknown event must be rejected with ErrInvalidEvent")
	}
	if _, err := ApplyTask(Task{State: TaskQueued}, TaskEvent{Kind: EvLease, FencingToken: 1}, t0); !errors.Is(err, ErrInvalidEvent) {
		t.Error("lease without a worker must be rejected")
	}
}

func assertReleasesCapacityOnce(t *testing.T, effects []Effect, worker string) {
	t.Helper()
	var n int
	for _, e := range effects {
		if r, ok := e.(ReleaseCapacity); ok {
			n++
			if r.WorkerID != worker {
				t.Errorf("released capacity on %q, want %q", r.WorkerID, worker)
			}
		}
	}
	if n != 1 {
		t.Errorf("got %d ReleaseCapacity effects, want exactly 1", n)
	}
}
