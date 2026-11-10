package app

import (
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/streamresume"

	"github.com/rs/zerolog/log"
)

// streamResumeEnabled reports whether any channel (without namespace or in a
// namespace) turns on stream resume. The cursor store is only created then,
// so default deployments keep their previous footprint.
func streamResumeEnabled(cfg config.Config) bool {
	if cfg.Channel.WithoutNamespace.StreamResume {
		return true
	}
	for _, ns := range cfg.Channel.Namespaces {
		if ns.StreamResume {
			return true
		}
	}
	return false
}

// newStreamResumeStore builds the cursor store selected by
// client.stream_resume.storage. Config validation already rejected unknown
// storage types and a file storage without a directory.
func newStreamResumeStore(cfg config.Config) (streamresume.Store, error) {
	if cfg.Client.StreamResume.Storage == "file" {
		log.Info().Str("dir", cfg.Client.StreamResume.FileDir).Msg("stream resume cursor store uses file storage")
		return streamresume.NewFileStore(cfg.Client.StreamResume.FileDir)
	}
	log.Info().Msg("stream resume cursor store uses in-memory storage")
	return streamresume.NewMemoryStore(), nil
}
