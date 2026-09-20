package workload

import (
	"encoding/json"
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

// DurationDistribution maps 95% of draws through an average-calibrated power
// curve below P95, 4% linearly from P95 to P99, and 1% exactly to P99.
type DurationDistribution struct {
	Average time.Duration
	P95     time.Duration
	P99     time.Duration
}

const maxPower = 10.0

type distributionJSON struct {
	Average float64 `json:"average"`
	P95     float64 `json:"p95"`
	P99     float64 `json:"p99"`
}

func (d DurationDistribution) MarshalJSON() ([]byte, error) {
	return json.Marshal(distributionJSON{
		Average: float64(d.Average) / float64(time.Millisecond),
		P95:     float64(d.P95) / float64(time.Millisecond),
		P99:     float64(d.P99) / float64(time.Millisecond),
	})
}

func (d *DurationDistribution) UnmarshalJSON(data []byte) error {
	var raw distributionJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	d.Average = time.Duration(raw.Average * float64(time.Millisecond))
	d.P95 = time.Duration(raw.P95 * float64(time.Millisecond))
	d.P99 = time.Duration(raw.P99 * float64(time.Millisecond))
	return d.Validate()
}

func (d DurationDistribution) Validate() error {
	if d.Average < 0 || d.P95 < 0 || d.P99 < 0 {
		return errors.New("workload: execution-time durations cannot be negative")
	}
	if d.P99 < d.P95 {
		return errors.New("workload: execution-time p99 cannot be below p95")
	}
	return nil
}

func (d DurationDistribution) Sample(rng *rand.Rand) time.Duration {
	quantile := rng.Float64()

	switch {
	case quantile < 0.95:
		if d.P95 <= 0 {
			return 0
		}
		uniform := quantile / 0.95
		return time.Duration(float64(d.P95) * math.Pow(uniform, d.power()))
	case quantile < 0.99:
		return interpolate(d.P95, d.P99, (quantile-0.95)/0.04)
	default:
		return d.P99
	}
}

// Head contribution is 0.95*P95/(power+1); tail contribution is fixed by the
// linear P95-P99 band and P99 point mass.
func (d DurationDistribution) power() float64 {
	if d.P95 <= 0 {
		return 1
	}
	tail := 0.04*float64(d.P95+d.P99)/2 + 0.01*float64(d.P99)
	head := float64(d.Average) - tail
	if head <= 0 {
		return maxPower
	}
	power := 0.95*float64(d.P95)/head - 1
	if power < 0 {
		return 0
	}
	if power > maxPower {
		return maxPower
	}
	return power
}

func interpolate(a, b time.Duration, fraction float64) time.Duration {
	if fraction <= 0 {
		return a
	}
	if fraction >= 1 {
		return b
	}
	return a + time.Duration(float64(b-a)*fraction)
}
