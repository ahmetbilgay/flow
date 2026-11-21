package n8n

import (
	"errors"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func TestJSMemoryLimit(t *testing.T) {
	rt := testRuntime(t)
	runtime.GC()
	rt.data.opts.JSMemoryLimit = int64(heapObjects() + 64<<20)

	// A run that keeps allocating is stopped and the runtime stays usable.
	start := time.Now()
	_, err := rt.runJS("const a = []; for (;;) a.push('x'.repeat(100000) + a.length);", 30*time.Second)
	if !errors.Is(err, errJSMemory) {
		t.Fatalf("want memory limit error, got %v", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("stopped too late: %v", d)
	}
	runtime.GC()
	if v, err := rt.runJS("let s = 0; for (let i = 0; i < 1000; i++) s += i; return s;", 5*time.Second); err != nil || v != 499500.0 {
		t.Fatalf("runtime unusable after memory stop: %v %v", v, err)
	}

	// Disabled guard, and the GOMEMLIMIT-derived default.
	if (&Options{JSMemoryLimit: -1}).jsMemoryLimit() != 0 {
		t.Fatal("negative must disable")
	}
	prev := debug.SetMemoryLimit(1 << 30)
	defer debug.SetMemoryLimit(prev)
	if got := (*Options)(nil).jsMemoryLimit(); got != (1<<30)/10*9 {
		t.Fatalf("GOMEMLIMIT default: %d", got)
	}
}
