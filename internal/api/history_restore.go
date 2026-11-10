package api

import (
	"context"
	"encoding/base64"
	"errors"
	"time"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/historyrestore"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
)

// HistorySnapshot builds a recovery package for a channel: every publication
// still retained, with its original offset, epoch, payload, tags and client
// info, plus a checksum used by history_restore to reject corruption.
func (h *Executor) HistorySnapshot(_ context.Context, cmd *HistorySnapshotRequest) *HistorySnapshotResponse {
	defer metrics.ObserveAPICommand(time.Now(), h.config.Protocol, "history_snapshot")

	resp := &HistorySnapshotResponse{}

	ch := cmd.Channel
	if ch == "" {
		resp.Error = ErrorBadRequest
		return resp
	}

	_, _, chOpts, found, err := h.cfgContainer.ChannelOptions(ch)
	if err != nil {
		resp.Error = ErrorInternal
		return resp
	}
	if !found {
		resp.Error = ErrorUnknownChannel
		return resp
	}
	if chOpts.HistorySize <= 0 || chOpts.HistoryTTL <= 0 {
		log.Warn().Str("channel", ch).Msg("history is not enabled for channel in Centrifugo configuration (history_size and history_ttl must both be > 0)")
		resp.Error = ErrorNotAvailable
		return resp
	}
	if h.restorer == nil {
		log.Warn().Str("channel", ch).Msg("history snapshot is not supported by the configured broker")
		resp.Error = ErrorNotAvailable
		return resp
	}

	pkg, err := h.restorer.SnapshotHistory(context.Background(), ch)
	if err != nil {
		log.Error().Err(err).Str("channel", ch).Msg("error building history snapshot")
		resp.Error = toRestoreAPIErr(err)
		return resp
	}

	resp.Result = snapshotResultFromPackage(pkg)
	return resp
}

// HistoryRestore validates a recovery package and fills the channel's missing
// publications at their original offsets. Identical publications already in
// the stream are kept, publications outside the package are untouched, and
// corruption, conflicts or write failures fail the whole call without changing
// the stream. Re-applying the same package is idempotent.
func (h *Executor) HistoryRestore(_ context.Context, cmd *HistoryRestoreRequest) *HistoryRestoreResponse {
	defer metrics.ObserveAPICommand(time.Now(), h.config.Protocol, "history_restore")

	resp := &HistoryRestoreResponse{}

	ch := cmd.Channel
	if ch == "" || cmd.Snapshot == nil {
		resp.Error = ErrorBadRequest
		return resp
	}

	_, _, chOpts, found, err := h.cfgContainer.ChannelOptions(ch)
	if err != nil {
		resp.Error = ErrorInternal
		return resp
	}
	if !found {
		resp.Error = ErrorUnknownChannel
		return resp
	}
	if chOpts.HistorySize <= 0 || chOpts.HistoryTTL <= 0 {
		log.Warn().Str("channel", ch).Msg("history is not enabled for channel in Centrifugo configuration (history_size and history_ttl must both be > 0)")
		resp.Error = ErrorNotAvailable
		return resp
	}
	if h.restorer == nil {
		log.Warn().Str("channel", ch).Msg("history restore is not supported by the configured broker")
		resp.Error = ErrorNotAvailable
		return resp
	}

	pkg, err := restorePackageFromProto(ch, cmd.Snapshot)
	if err != nil {
		log.Error().Err(err).Str("channel", ch).Msg("error decoding history restore package")
		resp.Error = ErrorBadRequest
		return resp
	}

	result, err := h.restorer.RestoreHistory(context.Background(), ch, pkg, historyrestore.Options{DryRun: cmd.DryRun})
	if err != nil {
		log.Error().Err(err).Str("channel", ch).Msg("error restoring history")
		resp.Error = toRestoreAPIErr(err)
		return resp
	}

	resp.Result = &HistoryRestoreResult{
		Total:          result.Total,
		Inserted:       result.Inserted,
		AlreadyPresent: result.AlreadyPresent,
		Offset:         result.Position.Offset,
		Epoch:          result.Position.Epoch,
		Complete:       result.Complete,
	}
	return resp
}

// toRestoreAPIErr maps history restore domain errors onto API errors.
// Corruption and malformed packages are client errors (107), a disagreement
// with the stored stream is a conflict (113); anything else is internal.
func toRestoreAPIErr(err error) *Error {
	switch {
	case errors.Is(err, historyrestore.ErrCorrupted), errors.Is(err, historyrestore.ErrInvalid):
		return ErrorBadRequest
	case errors.Is(err, historyrestore.ErrConflict):
		return ErrorConflict
	default:
		return ErrorInternal
	}
}

func snapshotResultFromPackage(pkg *historyrestore.Package) *HistorySnapshotResult {
	pubs := make([]*SnapshotPublication, 0, len(pkg.Publications))
	for _, rec := range pkg.Publications {
		pub := &SnapshotPublication{
			Offset: rec.Offset,
			Epoch:  rec.Epoch,
			Data:   Raw(rec.Data),
			Tags:   rec.Tags,
		}
		if rec.Info != nil {
			pub.Info = &ClientInfo{
				User:     rec.Info.UserID,
				Client:   rec.Info.ClientID,
				ConnInfo: Raw(rec.Info.ConnInfo),
				ChanInfo: Raw(rec.Info.ChanInfo),
			}
		}
		pubs = append(pubs, pub)
	}
	return &HistorySnapshotResult{
		Version:      uint32(pkg.Version),
		Channel:      pkg.Channel,
		Epoch:        pkg.Epoch,
		Offset:       pkg.Offset,
		CreatedAtMs:  pkg.CreatedAtMs,
		Checksum:     pkg.Checksum,
		Publications: pubs,
	}
}

func restorePackageFromProto(channel string, snap *HistorySnapshotResult) (*historyrestore.Package, error) {
	pkg := &historyrestore.Package{
		Version:      int(snap.Version),
		Channel:      snap.Channel,
		Epoch:        snap.Epoch,
		Offset:       snap.Offset,
		CreatedAtMs:  snap.CreatedAtMs,
		Checksum:     snap.Checksum,
		Publications: make([]historyrestore.Record, 0, len(snap.Publications)),
	}
	for _, pub := range snap.Publications {
		data := pub.Data
		if pub.B64Data != "" {
			decoded, err := base64.StdEncoding.DecodeString(pub.B64Data)
			if err != nil {
				return nil, err
			}
			data = Raw(decoded)
		}
		rec := historyrestore.Record{
			Offset: pub.Offset,
			Epoch:  pub.Epoch,
			Data:   data,
			Tags:   pub.Tags,
		}
		if pub.Info != nil {
			rec.Info = &centrifuge.ClientInfo{
				ClientID: pub.Info.Client,
				UserID:   pub.Info.User,
				ConnInfo: pub.Info.ConnInfo,
				ChanInfo: pub.Info.ChanInfo,
			}
		}
		pkg.Publications = append(pkg.Publications, rec)
	}
	// The broker rejects the package when its channel does not match the
	// request channel, so the channel argument is kept for call-site clarity.
	return pkg, nil
}
