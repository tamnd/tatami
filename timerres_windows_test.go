//go:build windows

package tatami

import "syscall"

// raiseTimerResolution asks Windows for a 1ms scheduler tick for the benchmark. The
// default multimedia timer runs at about 15.6ms, so a goroutine that yields or is
// briefly preempted does not resume until the next tick, which puts a 10-to-15ms
// floor under the tail of even a one-shard query. A Linux serving box ticks far
// finer and has no such floor, so timeBeginPeriod removes an artifact of the
// Windows measurement box rather than the engine.
func raiseTimerResolution() {
	winmm := syscall.NewLazyDLL("winmm.dll")
	proc := winmm.NewProc("timeBeginPeriod")
	if proc.Find() == nil {
		_, _, _ = proc.Call(uintptr(1))
	}
}
