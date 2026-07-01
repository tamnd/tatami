//go:build !windows

package tatami

// raiseTimerResolution is a no-op off Windows, where the scheduler already ticks
// finely enough that the benchmark tail reflects the engine and not the timer.
func raiseTimerResolution() {}
