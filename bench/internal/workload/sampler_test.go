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
			P95:     10 * time.Millisecond,
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

	// Simulate ID assignment for 1 vs 4 producers: strided by producer count.
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

func TestSampleRespectsBounds(t *testing.T) {
	d := testConfig().ExecutionTime
	rng := rand.New(rand.NewPCG(7, 8))
	for i := 0; i < 10000; i++ {
		sample := rng.Float64()
		_ = sample
		got := d.Sample(rng)
		if got < 0 || got > d.P99 {
			t.Fatalf("sample = %s, want in [0, %s]", got, d.P99)
		}
	}
}

func TestSamplePreservesP95Boundary(t *testing.T) {
	d := DurationDistribution{
		Average: 5 * time.Millisecond,
		P95:     10 * time.Millisecond,
		P99:     20 * time.Millisecond,
	}
	rng := rand.New(rand.NewPCG(9, 10))
	const n = 20000
	below := 0
	for i := 0; i < n; i++ {
		if d.Sample(rng) < d.P95 {
			below++
		}
	}
	// 95% of quantiles map below P95; allow sampling noise.
	ratio := float64(below) / n
	if ratio < 0.93 || ratio > 0.97 {
		t.Fatalf("below P95 ratio = %f, want ~0.95", ratio)
	}
}

func TestSampleIncorporatesAverage(t *testing.T) {
	low := DurationDistribution{
		Average: time.Millisecond,
		P95:     50 * time.Millisecond,
		P99:     100 * time.Millisecond,
	}
	high := DurationDistribution{
		Average: 20 * time.Millisecond,
		P95:     50 * time.Millisecond,
		P99:     100 * time.Millisecond,
	}
	if low.power() <= high.power() {
		t.Fatalf("low average power = %f, high = %f, want low > high", low.power(), high.power())
	}

	mean := func(d DurationDistribution) time.Duration {
		rng := rand.New(rand.NewPCG(11, 12))
		var total time.Duration
		const n = 20000
		for i := 0; i < n; i++ {
			total += d.Sample(rng)
		}
		return total / n
	}
	if mean(low) >= mean(high) {
		t.Fatal("low average distribution does not sample lower mean")
	}
}

func TestNorthStarDistributionTargets(t *testing.T) {
	distribution := DurationDistribution{
		Average: 250 * time.Millisecond,
		P95:     475 * time.Millisecond,
		P99:     646 * time.Millisecond,
	}
	rng := rand.New(rand.NewPCG(13, 14))
	const count = 100000
	samples := make([]time.Duration, count)
	var total time.Duration
	for i := range samples {
		samples[i] = distribution.Sample(rng)
		total += samples[i]
	}
	slices.Sort(samples)

	assertNear := func(name string, got, want, tolerance time.Duration) {
		t.Helper()
		difference := got - want
		if difference < 0 {
			difference = -difference
		}
		if difference > tolerance {
			t.Errorf("%s = %s, want %s +/- %s", name, got, want, tolerance)
		}
	}
	assertNear("average", total/count, distribution.Average, 5*time.Millisecond)
	assertNear("p95", samples[count*95/100-1], distribution.P95, 5*time.Millisecond)
	assertNear("p99", samples[count*99/100-1], distribution.P99, 5*time.Millisecond)
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

	// Deterministic selection for same seed.
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
		P95:     10 * time.Millisecond,
		P99:     20 * time.Millisecond,
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded DurationDistribution
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != original {
		t.Fatalf("decoded = %#v, want %#v", decoded, original)
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
	bad = good
	bad.ExecutionTime.P99 = time.Millisecond
	bad.ExecutionTime.P95 = 10 * time.Millisecond
	if err := bad.Validate(); err == nil {
		t.Fatal("expected p99 below p95 error")
	}
}
