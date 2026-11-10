//go:build windows

package streamresume

// syncDir is a no-op on Windows, where Rename is already atomic and
// directory fsync is not available.
func syncDir(_ string) {}
