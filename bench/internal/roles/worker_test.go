package roles

import (
	"context"
	"testing"
	"time"

	"github.com/raydatray/kq/bench/internal/workload"
)

func TestApplyFailureBehavior(t *testing.T) {
	if err := applyFailureBehavior(workload.Job{FailureMode: "none"}, time.Now()); err != nil {
		t.Fatalf("none = %v, want nil", err)
	}
	if err := applyFailureBehavior(workload.Job{}, time.Now()); err != nil {
		t.Fatalf("empty = %v, want nil", err)
	}
	if err := applyFailureBehavior(workload.Job{FailureMode: "permanent"}, time.Now()); err == nil {
		t.Fatal("permanent = nil, want error")
	}

	future := time.Now().Add(time.Minute)
	past := time.Now().Add(-time.Minute)
	if err := applyFailureBehavior(workload.Job{FailureMode: "until_time", FailUntil: future}, time.Now()); err == nil {
		t.Fatal("until_time future = nil, want error")
	}
	if err := applyFailureBehavior(workload.Job{FailureMode: "until_time", FailUntil: past}, time.Now()); err != nil {
		t.Fatalf("until_time past = %v, want nil", err)
	}
	if err := applyFailureBehavior(workload.Job{FailureMode: "attempts", FailAttempts: 1}, time.Now()); err == nil {
		t.Fatal("attempts = nil, want error until retry count is exposed")
	}
	if err := applyFailureBehavior(workload.Job{FailureMode: "bogus"}, time.Now()); err == nil {
		t.Fatal("unknown mode = nil, want error")
	}
}

func TestSleepRespectsContext(t *testing.T) {
	if err := sleep(context.Background(), 0); err != nil {
		t.Fatalf("sleep 0 = %v", err)
	}
	if err := sleep(context.Background(), 1); err != nil {
		t.Fatalf("sleep 1ms = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleep(ctx, 1000); err == nil {
		t.Fatal("canceled sleep = nil, want error")
	}
}
