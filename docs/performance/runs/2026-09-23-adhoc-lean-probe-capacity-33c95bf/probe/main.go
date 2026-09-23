//go:build ignore

// Minimal kq load probe used by this run. It drives kq.Client and kq.Worker
// unmodified and records only task IDs and queue times.
//
//	go build -o probe ./docs/performance/runs/2026-09-23-adhoc-lean-probe-capacity-33c95bf/probe/main.go
//	probe produce -clients 2 -goroutines 512 -rps 20000 -dur 60s
//	probe work -workers 8 -concurrency 1000 [-exec northstar|const:<ms>] -out ids-0.bin
//	probe check ids-*.bin
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/raydatray/kq"
)

const queue = "probe"

func cfg() kq.Config {
	policy, _ := kq.NewRetryPolicy(0, func(int32, string) time.Duration { return 0 })
	return kq.Config{Brokers: []string{"localhost:19092"}, Queue: queue, RetryPolicy: policy}
}

func main() {
	switch os.Args[1] {
	case "produce":
		produce(os.Args[2:])
	case "work":
		work(os.Args[2:])
	case "check":
		check(os.Args[2:])
	}
}

func produce(args []string) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	clients := fs.Int("clients", 1, "kq.Client instances")
	perClient := fs.Int("goroutines", 512, "concurrent Enqueue callers per client")
	rps := fs.Int("rps", 12000, "target total enqueues/s")
	dur := fs.Duration("dur", 30*time.Second, "duration")
	fs.Parse(args)

	total := uint64(*rps) * uint64(dur.Seconds())
	callers := *clients * *perClient
	var next, done, errs atomic.Uint64
	var wg sync.WaitGroup
	start := time.Now().Add(200 * time.Millisecond)
	for c := 0; c < *clients; c++ {
		client, err := kq.NewClient(cfg())
		if err != nil {
			panic(err)
		}
		defer client.Close()
		for g := 0; g < *perClient; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				payload := make([]byte, 200)
				for {
					id := next.Add(1) - 1
					if id >= total {
						return
					}
					due := start.Add(time.Duration(float64(id) / float64(*rps) * float64(time.Second)))
					if d := time.Until(due); d > 0 {
						time.Sleep(d)
					}
					binary.LittleEndian.PutUint64(payload[0:], id)
					binary.LittleEndian.PutUint64(payload[8:], uint64(time.Now().UnixNano()))
					if _, err := client.Enqueue(context.Background(), kq.Task{Type: "probe", Payload: payload}); err != nil {
						errs.Add(1)
						continue
					}
					done.Add(1)
				}
			}()
		}
	}
	_ = callers
	wg.Wait()
	el := time.Since(start).Seconds()
	fmt.Printf("produce: requested=%d/s achieved=%.0f/s total=%d errs=%d\n", *rps, float64(done.Load())/el, done.Load(), errs.Load())
}

// north-star: lognormal, mean 250 ms, p95 ~475 ms, p99 ~646 ms.
var (
	sigma  = math.Log(646.0/475.0) / (2.326 - 1.645)
	median = 250.0 / math.Exp(sigma*sigma/2)
)

func execTime(id uint64) time.Duration {
	r := rand.New(rand.NewPCG(id, 0x9e3779b97f4a7c15))
	ms := median * math.Exp(sigma*r.NormFloat64())
	return time.Duration(ms * float64(time.Millisecond))
}

func pick(mode string, id uint64) time.Duration {
	if strings.HasPrefix(mode, "const:") {
		ms, _ := strconv.Atoi(strings.TrimPrefix(mode, "const:"))
		return time.Duration(ms) * time.Millisecond
	}
	return execTime(id)
}

func work(args []string) {
	fs := flag.NewFlagSet("work", flag.ExitOnError)
	workers := fs.Int("workers", 4, "kq.Worker instances in this process")
	concurrency := fs.Int("concurrency", 1000, "Worker concurrency")
	out := fs.String("out", "ids.bin", "completed ids output")
	idle := fs.Duration("idle", 15*time.Second, "exit after this long with no completions (after first)")
	execMode := fs.String("exec", "northstar", "northstar | const:<ms>")
	fs.Parse(args)

	var mu sync.Mutex
	ids := make([]uint64, 0, 1<<20)
	queueMS := make([]float32, 0, 1<<20)
	var completed atomic.Int64
	var lastDone atomic.Int64

	handler := func(ctx context.Context, task kq.Task) error {
		id := binary.LittleEndian.Uint64(task.Payload[0:])
		enq := int64(binary.LittleEndian.Uint64(task.Payload[8:]))
		q := float32(time.Now().UnixNano()-enq) / 1e6
		select {
		case <-time.After(pick(*execMode, id)):
		case <-ctx.Done():
			return ctx.Err()
		}
		mu.Lock()
		ids = append(ids, id)
		queueMS = append(queueMS, q)
		mu.Unlock()
		completed.Add(1)
		lastDone.Store(time.Now().UnixNano())
		return nil
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	var wg sync.WaitGroup
	for range *workers {
		w, err := kq.NewWorker(kq.WorkerConfig{Config: cfg(), Concurrency: *concurrency}, handler)
		if err != nil {
			panic(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "worker error:", err)
			}
		}()
	}
	fmt.Fprintln(os.Stderr, "ready")

	var prev int64
	var peak float64
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		cur := completed.Load()
		rate := float64(cur - prev)
		prev = cur
		if rate > peak {
			peak = rate
		}
		fmt.Fprintf(os.Stderr, "%s completed=%d rate=%.0f/s\n", time.Now().Format("15:04:05"), cur, rate)
		if l := lastDone.Load(); l > 0 && time.Since(time.Unix(0, l)) > *idle {
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	f, _ := os.Create(*out)
	buf := make([]byte, 8)
	for _, id := range ids {
		binary.LittleEndian.PutUint64(buf, id)
		f.Write(buf)
	}
	f.Close()
	qf, _ := os.Create(*out + ".queue")
	for _, q := range queueMS {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(q))
		qf.Write(buf[:4])
	}
	qf.Close()
	fmt.Printf("work: completed=%d peak_1s_rate=%.0f/s\n", len(ids), peak)
}

func check(files []string) {
	seen := map[uint64]int{}
	var queueMS []float64
	var maxID uint64
	for _, name := range files {
		data, _ := os.ReadFile(name)
		for i := 0; i+8 <= len(data); i += 8 {
			id := binary.LittleEndian.Uint64(data[i:])
			seen[id]++
			maxID = max(maxID, id)
		}
		qd, _ := os.ReadFile(name + ".queue")
		for i := 0; i+4 <= len(qd); i += 4 {
			queueMS = append(queueMS, float64(math.Float32frombits(binary.LittleEndian.Uint32(qd[i:]))))
		}
	}
	dups := 0
	for _, n := range seen {
		if n > 1 {
			dups += n - 1
		}
	}
	missing := int(maxID+1) - len(seen)
	slices.Sort(queueMS)
	p := func(q float64) float64 {
		if len(queueMS) == 0 {
			return 0
		}
		return queueMS[int(q*float64(len(queueMS)-1))]
	}
	fmt.Printf("check: unique=%d dups=%d missing(<=max id)=%d queue p50/p95/p99/max=%.0f/%.0f/%.0f/%.0f ms\n",
		len(seen), dups, missing, p(0.5), p(0.95), p(0.99), p(1))
}
