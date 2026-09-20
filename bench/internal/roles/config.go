package roles

import (
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/raydatray/kq"
	"github.com/raydatray/kq/bench/internal/workload"
)

type Config struct {
	KQ           KQConfig       `json:"kq"`
	Topology     TopologyConfig `json:"topology"`
	Workload     WorkloadConfig `json:"workload"`
	ProcessIndex int            `json:"-"`
}

type KQConfig struct {
	Brokers               []string `json:"brokers"`
	Queue                 string   `json:"queue"`
	MaxRetries            int32    `json:"max_retries"`
	RetryDelayMS          int64    `json:"retry_delay_ms"`
	RetryGridBoundariesMS []int64  `json:"retry_grid_boundaries_ms"`
	RetryGridPartitions   int32    `json:"retry_grid_partitions"`
}

type TopologyConfig struct {
	ReadyPartitions   int `json:"ready_partitions,omitempty"`
	RetryPartitions   int `json:"retry_partitions,omitempty"`
	Producers         int `json:"producers"`
	Workers           int `json:"workers"`
	WorkerConcurrency int `json:"worker_concurrency"`
	Movers            int `json:"movers"`
}

type WorkloadConfig struct {
	Arrival       ArrivalConfig                 `json:"arrival"`
	ExecutionTime workload.DurationDistribution `json:"execution_time_ms"`
	Failures      workload.FailureConfig        `json:"failures"`
	RandomSeed    uint64                        `json:"random_seed"`
	Warmup        WarmupConfig                  `json:"warmup"`
}

type WarmupConfig struct {
	Tasks  int    `json:"tasks"`
	IDBase uint64 `json:"id_base"`
}

type ArrivalConfig struct {
	TargetRPS       int `json:"target_rps"`
	DurationSeconds int `json:"duration_seconds"`
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return Config{}, err
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Validate() error {
	if len(c.KQ.Brokers) == 0 {
		return errors.New("roles: at least one broker is required")
	}
	if c.KQ.Queue == "" {
		return errors.New("roles: queue name cannot be empty")
	}
	if c.KQ.MaxRetries < 0 {
		return errors.New("roles: max retries cannot be negative")
	}
	if c.KQ.RetryDelayMS < 0 {
		return errors.New("roles: retry delay cannot be negative")
	}
	if c.Topology.Producers <= 0 {
		return errors.New("roles: producer count must be positive")
	}
	if c.Topology.Workers <= 0 {
		return errors.New("roles: worker count must be positive")
	}
	if c.Topology.WorkerConcurrency <= 0 {
		return errors.New("roles: worker concurrency must be positive")
	}
	if c.Topology.Movers <= 0 {
		return errors.New("roles: mover count must be positive")
	}
	if c.ProcessIndex < 0 {
		return errors.New("roles: process index cannot be negative")
	}
	if c.Workload.Arrival.TargetRPS <= 0 {
		return errors.New("roles: target RPS must be positive")
	}
	if c.Workload.Arrival.DurationSeconds <= 0 {
		return errors.New("roles: arrival duration must be positive")
	}
	if c.Workload.Warmup.Tasks < 0 {
		return errors.New("roles: warm-up task count cannot be negative")
	}
	return c.ToWorkloadConfig().Validate()
}

func (c Config) TotalTasks() uint64 {
	return uint64(c.Workload.Arrival.TargetRPS) * uint64(c.Workload.Arrival.DurationSeconds)
}

func (c Config) WorkloadIDs() []uint64 {
	total := c.TotalTasks()
	var ids []uint64
	for id := uint64(c.ProcessIndex); id < total; id += uint64(c.Topology.Producers) {
		ids = append(ids, id)
	}
	return ids
}

func (c Config) DueAt(started time.Time, id uint64) time.Time {
	return started.Add(time.Duration(id) * time.Second / time.Duration(c.Workload.Arrival.TargetRPS))
}

func (c Config) WarmupIDs() []uint64 {
	var ids []uint64
	for i := 0; i < c.Workload.Warmup.Tasks; i++ {
		ids = append(ids, c.Workload.Warmup.IDBase+uint64(i))
	}
	return ids
}

func (c Config) ToWorkloadConfig() workload.Config {
	return workload.Config{
		RandomSeed:    c.Workload.RandomSeed,
		ExecutionTime: c.Workload.ExecutionTime,
		Failures:      c.Workload.Failures,
	}
}

func (k KQConfig) ToKQConfig() (kq.Config, error) {
	delay := time.Duration(k.RetryDelayMS) * time.Millisecond
	policy, err := kq.NewRetryPolicy(k.MaxRetries, func(int32, string) time.Duration {
		return delay
	})
	if err != nil {
		return kq.Config{}, err
	}
	var grid kq.RetryGrid
	if k.MaxRetries > 0 {
		boundaries := make([]time.Duration, 0, len(k.RetryGridBoundariesMS))
		for _, ms := range k.RetryGridBoundariesMS {
			boundaries = append(boundaries, time.Duration(ms)*time.Millisecond)
		}
		var err error
		grid, err = kq.NewRetryGrid(boundaries, k.RetryGridPartitions)
		if err != nil {
			return kq.Config{}, err
		}
	}
	return kq.Config{
		Brokers:     k.Brokers,
		Queue:       k.Queue,
		RetryPolicy: policy,
		RetryGrid:   grid,
	}, nil
}
