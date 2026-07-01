//go:build !darwin && !linux

package search

import "os"

// openMapped reads the whole file into a heap slice on platforms without a mmap
// path here (notably Windows). It gives the same aliased columns as the real
// mmap, correct but resident rather than demand-paged (scale/11 lever three). The
// serving boxes are Linux, so this is a portability floor, not the fast path.
func openMapped(path string) (data []byte, closer func() error, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	return b, func() error { return nil }, nil
}
