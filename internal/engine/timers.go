package engine

import (
	"fmt"
	"time"
)

// timerAction marks WAITING steps that resume by themselves at DueAt.
const timerAction = "__timer"

// WaitUntilError is returned by an executor that wants to pause until a
// point in time without blocking a worker: the step is stored as WAITING
// with Output already computed, and the timeout reaper completes it once
// At has passed (durable across restarts).
type WaitUntilError struct {
	At     time.Time
	Output string
}

func (w *WaitUntilError) Error() string {
	return fmt.Sprintf("WAIT_UNTIL %s", w.At.UTC().Format(time.RFC3339))
}
