package roles

import (
	"testing"
	"time"
)

func testProducerConfig() Config {
	config := Config{
		KQ: KQConfig{
			Brokers:                 []string{"localhost:19092"},
			Queue:                   "bench",
			MaxRetries:              3,
			RetryDelayMS:            1000,
			RetryGridBoundariesMS:   []int64{0, 120000},
			RetryGridPartitions:     4,
		},
		Topology: TopologyConfig{
			Producers: 2,
			Workers:   1,
			Movers:    1,
		},
		Workload: WorkloadConfig{
			Arrival: ArrivalConfig{
				TargetRPS:       100,
				DurationSeconds: 5,
			},
		},
	}
	return config
}

func TestTotalTasks(t *testing.T) {
	config := testProducerConfig()
	if got, want := config.TotalTasks(), uint64(500); got != want {
		t.Fatalf("TotalTasks = %d, want %d", got, want)
	}
}

func TestWorkloadIDsSplit(t *testing.T) {
	config := testProducerConfig()
	config.Workload.Arrival.TargetRPS = 10
	config.Workload.Arrival.DurationSeconds = 1 // 10 tasks

	config.ProcessIndex = 0
	zero := config.WorkloadIDs()
	config.ProcessIndex = 1
	one := config.WorkloadIDs()

	if len(zero)+len(one) != 10 {
		t.Fatalf("split sizes = %d+%d, want 10", len(zero), len(one))
	}
	seen := make(map[uint64]bool)
	for _, id := range append(zero, one...) {
		if seen[id] {
			t.Fatalf("duplicate ID %d", id)
		}
		seen[id] = true
	}
	for id := uint64(0); id < 10; id++ {
		if !seen[id] {
			t.Fatalf("missing ID %d", id)
		}
	}
	// Strided assignment preserves order per producer.
	for i, id := range zero {
		if id != uint64(i*2) {
			t.Fatalf("producer 0 ID %d = %d", i, id)
		}
	}
}

func TestDueAtPacing(t *testing.T) {
	config := testProducerConfig()
	config.Workload.Arrival.TargetRPS = 100
	started := time.Now()
	if got := config.DueAt(started, 0); !got.Equal(started) {
		t.Fatalf("DueAt(0) = %s, want %s", got, started)
	}
	if got, want := config.DueAt(started, 100), started.Add(time.Second); !got.Equal(want) {
		t.Fatalf("DueAt(100) = %s, want %s", got, want)
	}
}

func TestConfigValidation(t *testing.T) {
	good := testProducerConfig()
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := good
	bad.Workload.Arrival.TargetRPS = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("expected target RPS error")
	}
	bad = good
	bad.Topology.Producers = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("expected producer count error")
	}
	bad = good
	bad.KQ.Brokers = nil
	if err := bad.Validate(); err == nil {
		t.Fatal("expected brokers error")
	}
}

func TestToKQConfig(t *testing.T) {
	config := testProducerConfig()
	kqConfig, err := config.KQ.ToKQConfig()
	if err != nil {
		t.Fatal(err)
	}
	if len(kqConfig.Brokers) != 1 || kqConfig.Queue != "bench" {
		t.Fatalf("KQ config = %#v", kqConfig)
	}

	// Zero retries needs no grid.
	config.KQ.MaxRetries = 0
	if _, err := config.KQ.ToKQConfig(); err != nil {
		t.Fatal(err)
	}
}
