package tatami

import "errors"

// ErrNotFound is returned by point-lookup paths when a key is absent. The CLI
// maps it to a distinct exit code so callers can tell a clean miss from a real
// failure, matching the fleet convention.
var ErrNotFound = errors.New("tatami: not found")

// ErrClosed is returned when a Writer is used after Close.
var ErrClosed = errors.New("tatami: writer is closed")
