//go:build darwin || linux

package search

import (
	"os"
	"syscall"
)

// openMapped memory-maps a file read-only and returns the mapped bytes with an
// unmap closer. On the Linux serving boxes and the darwin dev box this is a real
// mmap, so a routing.bin larger than RAM serves from the page cache and only the
// pages a query touches are brought in (scale/11 lever three). The mapping is
// PROT_READ, so the aliased columns are never written through, and the base is
// page aligned, so the fixed-width sections alias at their natural alignment.
func openMapped(path string) (data []byte, closer func() error, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	n := int(fi.Size())
	if n == 0 {
		return nil, func() error { return nil }, nil
	}
	b, err := syscall.Mmap(int(f.Fd()), 0, n, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	return b, func() error { return syscall.Munmap(b) }, nil
}
