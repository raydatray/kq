package workload

import (
	"encoding/json"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		RandomSeed: 1,
		ExecutionTime: DurationDistribution{
			Average: 5 * time.Millisecond,
			P99:     20 * time.Millisecond,
		},
		Failures: FailureConfig{
			Rate: 0.01,
			Mode: "permanent",
		},
	}
}

func TestGenerateDeterministic(t *testing.T) {
	config := testConfig()
	a := Generate(config, 42)
	b := Generate(config, 42)
	if a != b {
		t.Fatalf("Generate not deterministic: %#v vs %#v", a, b)
	}
	if a.WorkloadID != 42 {
		t.Fatalf("WorkloadID = %d, want 42", a.WorkloadID)
	}
}

func TestGenerateIndependentOfProducerCount(t *testing.T) {
	config := testConfig()
	const total = 100

	assigned := func(producerCount int) map[uint64]Job {
		out := make(map[uint64]Job)
		for index := 0; index < producerCount; index++ {
			for id := uint64(index); id < total; id += uint64(producerCount) {
				out[id] = Generate(config, id)
			}
		}
		return out
	}

	single := assigned(1)
	quad := assigned(4)
	if len(single) != total || len(quad) != total {
		t.Fatalf("assigned = %d/%d, want %d", len(single), len(quad), total)
	}
	for id := uint64(0); id < total; id++ {
		if single[id] != quad[id] {
			t.Fatalf("ID %d differs across producer counts", id)
		}
	}
}

func TestGenerateFailureIndependentOfDuration(t *testing.T) {
	first := testConfig()
	first.Failures.Rate = 0.5
	second := first
	second.ExecutionTime = DurationDistribution{
		Average: 250 * time.Millisecond,
		P99:     646 * time.Millisecond,
	}

	for id := uint64(0); id < 1000; id++ {
		if a, b := Generate(first, id), Generate(second, id); a.FailureMode != b.FailureMode {
			t.Fatalf("workload %d failure modes = %q/%q", id, a.FailureMode, b.FailureMode)
		}
	}
}

func TestSampleDeterministic(t *testing.T) {
	d := testConfig().ExecutionTime
	rngA := rand.New(rand.NewPCG(1, 2))
	rngB := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 100; i++ {
		if a, b := d.Sample(rngA), d.Sample(rngB); a != b {
			t.Fatalf("Sample not deterministic at %d: %s vs %s", i, a, b)
		}
	}
}

func TestNorthStarDistributionTargets(t *testing.T) {
	distribution := DurationDistribution{
		Average: 250 * time.Millisecond,
		P95:     475 * time.Millisecond,
		P99:     646 * time.Millisecond,
	}
	if err := distribution.Validate(); err != nil {
		t.Fatal(err)
	}
	average, p95, p99, _ := sampleSummary(distribution, 100000)
	assertNear(t, "average", average, distribution.Average, 5*time.Millisecond)
	assertNear(t, "p95", p95, distribution.P95, 5*time.Millisecond)
	assertNear(t, "p99", p99, distribution.P99, 10*time.Millisecond)
}

func TestP95OnlyDistributionTargets(t *testing.T) {
	distribution := DurationDistribution{
		Average: 250 * time.Millisecond,
		P95:     475 * time.Millisecond,
	}
	if err := distribution.Validate(); err != nil {
		t.Fatal(err)
	}
	average, p95, _, _ := sampleSummary(distribution, 100000)
	assertNear(t, "average", average, distribution.Average, 5*time.Millisecond)
	assertNear(t, "p95", p95, distribution.P95, 5*time.Millisecond)
}

func TestAverageOnlyDistributionIsConstant(t *testing.T) {
	distribution := DurationDistribution{Average: 250 * time.Millisecond}
	rng := rand.New(rand.NewPCG(15, 16))
	for range 100 {
		if got := distribution.Sample(rng); got != distribution.Average {
			t.Fatalf("sample = %s, want %s", got, distribution.Average)
		}
	}
}

func TestDistributionMaximum(t *testing.T) {
	distribution := DurationDistribution{
		Average: 250 * time.Millisecond,
		P99:     646 * time.Millisecond,
		Max:     700 * time.Millisecond,
	}
	if err := distribution.Validate(); err != nil {
		t.Fatal(err)
	}
	_, _, _, maximum := sampleSummary(distribution, 100000)
	if maximum != distribution.Max {
		t.Fatalf("maximum = %s, want %s", maximum, distribution.Max)
	}
}

func sampleSummary(distribution DurationDistribution, count int) (time.Duration, time.Duration, time.Duration, time.Duration) {
	rng := rand.New(rand.NewPCG(13, 14))
	samples := make([]time.Duration, count)
	var total time.Duration
	for i := range samples {
		samples[i] = distribution.Sample(rng)
		total += samples[i]
	}
	slices.Sort(samples)
	return total / time.Duration(count), samples[count*95/100-1], samples[count*99/100-1], samples[count-1]
}

func assertNear(t *testing.T, name string, got, want, tolerance time.Duration) {
	t.Helper()
	difference := got - want
	if difference < 0 {
		difference = -difference
	}
	if difference > tolerance {
		t.Errorf("%s = %s, want %s +/- %s", name, got, want, tolerance)
	}
}

func TestModeForSelection(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 1))
	if got := (FailureConfig{Rate: 0, Mode: "permanent"}).ModeFor(rng); got != "none" {
		t.Fatalf("rate 0 mode = %q, want none", got)
	}
	if got := (FailureConfig{Rate: 1, Mode: "permanent"}).ModeFor(rng); got != "permanent" {
		t.Fatalf("rate 1 mode = %q, want permanent", got)
	}
	if got := (FailureConfig{Rate: 1, Mode: "none"}).ModeFor(rng); got != "none" {
		t.Fatalf("none mode = %q, want none", got)
	}

	config := FailureConfig{Rate: 0.5, Mode: "permanent"}
	rngA := rand.New(rand.NewPCG(3, 4))
	rngB := rand.New(rand.NewPCG(3, 4))
	for i := 0; i < 100; i++ {
		if a, b := config.ModeFor(rngA), config.ModeFor(rngB); a != b {
			t.Fatalf("ModeFor not deterministic at %d", i)
		}
	}
}

func TestDurationJSON(t *testing.T) {
	original := DurationDistribution{
		Average: 5 * time.Millisecond,
		P99:     20 * time.Millisecond,
		Max:     100 * time.Millisecond,
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if _, ok := wire["p95"]; ok {
		t.Fatal("unset p95 was encoded")
	}
	var decoded DurationDistribution
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != original {
		t.Fatalf("decoded = %#v, want %#v", decoded, original)
	}
	if err := json.Unmarshal([]byte(`{"p99":20}`), &decoded); err == nil {
		t.Fatal("missing average accepted")
	}
}

func TestConfigValidation(t *testing.T) {
	good := testConfig()
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := good
	bad.Failures.Mode = "bogus"
	if err := bad.Validate(); err == nil {
		t.Fatal("expected unknown mode error")
	}
	bad = good
	bad.Failures.Rate = 2
	if err := bad.Validate(); err == nil {
		t.Fatal("expected rate range error")
	}
	tests := []DurationDistribution{
		{Average: -time.Millisecond},
		{P99: time.Millisecond},
		{Average: 10 * time.Millisecond, P95: 5 * time.Millisecond},
		{Average: 5 * time.Millisecond, P95: 10 * time.Millisecond, P99: 9 * time.Millisecond},
		{Average: 5 * time.Millisecond, P99: 20 * time.Millisecond, Max: 10 * time.Millisecond},
		{Average: time.Millisecond, P99: 100 * time.Millisecond},
		{Average: 5 * time.Millisecond, P95: 10 * time.Millisecond, P99: 20 * time.Millisecond},
	}
	for _, distribution := range tests {
		bad = good
		bad.ExecutionTime = distribution
		if err := bad.Validate(); err == nil {
			t.Errorf("distribution %#v accepted", distribution)
		}
	}
}
