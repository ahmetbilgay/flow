package n8n

import (
	"math"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"time"
)

// goja cannot account memory per runtime, so JavaScript runs are bounded by
// the process heap instead: while any run is active, a monitor samples the
// heap and, when it exceeds a run's limit (after a forced GC to discard
// garbage), signals that run to stop. Concurrent runs with the same limit
// stop together; their steps fail with a retryable error.

const (
	jsMemSample  = 20 * time.Millisecond
	jsMemGCEvery = time.Second
	heapMetric   = "/memory/classes/heap/objects:bytes"
)

type jsMemRun struct {
	limit uint64
	hit   chan struct{}
	fired bool
}

var jsMem = struct {
	sync.Mutex
	runs    map[*jsMemRun]struct{}
	running bool
	lastGC  time.Time
}{runs: map[*jsMemRun]struct{}{}}

// jsMemoryLimit resolves Options.JSMemoryLimit: positive is used as is,
// negative disables the guard, zero means 90% of the Go memory limit
// (GOMEMLIMIT / debug.SetMemoryLimit) when one is set, otherwise disabled.
func (o *Options) jsMemoryLimit() uint64 {
	if o != nil && o.JSMemoryLimit != 0 {
		if o.JSMemoryLimit < 0 {
			return 0
		}
		return uint64(o.JSMemoryLimit)
	}
	if l := debug.SetMemoryLimit(-1); l > 0 && l < math.MaxInt64 {
		return uint64(l) / 10 * 9
	}
	return 0
}

// watchJSMemory registers a run; the returned channel is closed when the
// heap exceeds limit. stop must be called when the run ends.
func watchJSMemory(limit uint64) (hit <-chan struct{}, stop func()) {
	if limit == 0 {
		return nil, func() {}
	}
	run := &jsMemRun{limit: limit, hit: make(chan struct{})}
	jsMem.Lock()
	jsMem.runs[run] = struct{}{}
	if !jsMem.running {
		jsMem.running = true
		go jsMemMonitor()
	}
	jsMem.Unlock()
	return run.hit, func() {
		jsMem.Lock()
		delete(jsMem.runs, run)
		jsMem.Unlock()
	}
}

func heapObjects() uint64 {
	s := []metrics.Sample{{Name: heapMetric}}
	metrics.Read(s)
	if s[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return s[0].Value.Uint64()
}

func jsMemMonitor() {
	t := time.NewTicker(jsMemSample)
	defer t.Stop()
	for range t.C {
		jsMem.Lock()
		if len(jsMem.runs) == 0 {
			jsMem.running = false
			jsMem.Unlock()
			return
		}
		lowest := uint64(math.MaxUint64)
		for r := range jsMem.runs {
			if !r.fired && r.limit < lowest {
				lowest = r.limit
			}
		}
		jsMem.Unlock()
		heap := heapObjects()
		if heap <= lowest {
			continue
		}
		// The sample includes unswept garbage; collect before blaming a run.
		jsMem.Lock()
		gcDue := time.Since(jsMem.lastGC) >= jsMemGCEvery
		if gcDue {
			jsMem.lastGC = time.Now()
		}
		jsMem.Unlock()
		if gcDue {
			runtime.GC()
			heap = heapObjects()
		}
		jsMem.Lock()
		for r := range jsMem.runs {
			if !r.fired && heap > r.limit {
				r.fired = true
				close(r.hit)
			}
		}
		jsMem.Unlock()
	}
}
