//go:build !windows

package streamresume

import "os"

// syncDir flushes directory metadata so a rename inside it is durable.
// Errors are best-effort: the renamed cursor file itself was already synced.
func syncDir(dir string) {
	// The directory is configured by the operator, not derived from request
	// data, so opening a variable path here is intentional.
	d, err := os.Open(dir) //nolint:gosec // G304: trusted operator-configured path.
	if err != nil {
		return
	}
	defer func() { _ = d.Close() }()
	_ = d.Sync()
}
