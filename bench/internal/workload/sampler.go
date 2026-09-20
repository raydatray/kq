package workload

import (
	"encoding/json"
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

type DurationDistribution struct {
	Average time.Duration
	P95     time.Duration
	P99     time.Duration
	Max     time.Duration
}

const (
	z95 = 1.644853627
	z99 = 2.326347874
)

type distributionJSON struct {
	Average *float64 `json:"average"`
	P95     *float64 `json:"p95,omitempty"`
	P99     *float64 `json:"p99,omitempty"`
	Max     *float64 `json:"max,omitempty"`
}

func (d DurationDistribution) MarshalJSON() ([]byte, error) {
	average := milliseconds(d.Average)
	return json.Marshal(distributionJSON{
		Average: &average,
		P95:     optionalMilliseconds(d.P95),
		P99:     optionalMilliseconds(d.P99),
		Max:     optionalMilliseconds(d.Max),
	})
}

func (d *DurationDistribution) UnmarshalJSON(data []byte) error {
	var raw distributionJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Average == nil {
		return errors.New("workload: execution-time average is required")
	}
	*d = DurationDistribution{Average: fromMilliseconds(raw.Average)}
	d.P95 = fromMilliseconds(raw.P95)
	d.P99 = fromMilliseconds(raw.P99)
	d.Max = fromMilliseconds(raw.Max)
	return d.Validate()
}

func (d DurationDistribution) Validate() error {
	if d.Average < 0 || d.P95 < 0 || d.P99 < 0 || d.Max < 0 {
		return errors.New("workload: execution-time durations cannot be negative")
	}
	if d.Average == 0 {
		if d.P95 != 0 || d.P99 != 0 || d.Max != 0 {
			return errors.New("workload: zero average cannot have percentiles or maximum")
		}
		return nil
	}
	if d.P95 > 0 && d.P95 < d.Average {
		return errors.New("workload: execution-time p95 cannot be below average")
	}
	if d.P99 > 0 && d.P99 < d.Average {
		return errors.New("workload: execution-time p99 cannot be below average")
	}
	if d.P95 > 0 && d.P99 > 0 && d.P99 < d.P95 {
		return errors.New("workload: execution-time p99 cannot be below p95")
	}
	if d.Max > 0 {
		if d.Max < d.Average {
			return errors.New("workload: execution-time maximum cannot be below average")
		}
		if d.P95 > 0 && d.Max < d.P95 {
			return errors.New("workload: execution-time maximum cannot be below p95")
		}
		if d.P99 > 0 && d.Max < d.P99 {
			return errors.New("workload: execution-time maximum cannot be below p99")
		}
	}
	mu, sigma, err := d.parameters()
	if err != nil {
		return err
	}
	if d.P95 > 0 && d.P99 > 0 {
		implied := math.Exp(mu + z95*sigma)
		difference := math.Abs(math.Log(implied / float64(d.P95)))
		if difference > math.Log(1.05) {
			return errors.New("workload: execution-time p95 is inconsistent with average and p99")
		}
	}
	return nil
}

func (d DurationDistribution) Sample(rng *rand.Rand) time.Duration {
	if d.Average == 0 {
		return 0
	}
	if d.P95 == 0 && d.P99 == 0 {
		return d.Average
	}

	mu, sigma, _ := d.parameters()
	u1 := max(rng.Float64(), math.SmallestNonzeroFloat64)
	u2 := rng.Float64()
	normal := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
	duration := time.Duration(math.Exp(mu + sigma*normal))
	if d.Max > 0 {
		duration = min(duration, d.Max)
	}
	return duration
}

func (d DurationDistribution) parameters() (mu, sigma float64, err error) {
	switch {
	case d.P99 > 0:
		sigma, err = sigmaFor(d.Average, d.P99, z99)
	case d.P95 > 0:
		sigma, err = sigmaFor(d.Average, d.P95, z95)
	}
	if err != nil {
		return 0, 0, err
	}
	mu = math.Log(float64(d.Average)) - sigma*sigma/2
	return mu, sigma, nil
}

func sigmaFor(mean, percentile time.Duration, z float64) (float64, error) {
	if percentile < mean {
		return 0, errors.New("workload: execution-time percentile cannot be below average")
	}
	discriminant := z*z - 2*math.Log(float64(percentile)/float64(mean))
	if discriminant < 0 {
		return 0, errors.New("workload: execution-time percentile is incompatible with average")
	}
	return z - math.Sqrt(discriminant), nil
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func optionalMilliseconds(duration time.Duration) *float64 {
	if duration == 0 {
		return nil
	}
	value := milliseconds(duration)
	return &value
}

func fromMilliseconds(value *float64) time.Duration {
	if value == nil {
		return 0
	}
	return time.Duration(*value * float64(time.Millisecond))
}
