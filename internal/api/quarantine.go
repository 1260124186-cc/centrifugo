package api

import (
	"context"
	"errors"
	"time"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/quarantine"

	"github.com/rs/zerolog/log"
)

// SetQuarantineService wires the consumer quarantine area into the executor.
// It enables the consumer_quarantine_list and consumer_quarantine_replay API
// commands. The same service is shared by the HTTP and gRPC executors.
func (h *Executor) SetQuarantineService(service *quarantine.Service) {
	h.quarantine = service
}

// quarantineError maps a quarantine service error onto an API error. A
// corrupted record or an undeterminable source is a client-visible bad
// request with the precise reason; anything else is an internal error.
func quarantineError(err error) *Error {
	if errors.Is(err, quarantine.ErrCorrupted) || errors.Is(err, quarantine.ErrUnknownSource) {
		return &Error{Code: ErrorBadRequest.Code, Message: err.Error(), Temporary: false}
	}
	return ErrorInternal
}

// toAPIQuarantineRecord converts a quarantine record into the API representation.
func toAPIQuarantineRecord(record quarantine.Record) *ConsumerQuarantineRecord {
	return &ConsumerQuarantineRecord{
		Id:           record.ID,
		Consumer:     record.Consumer,
		ConsumerType: record.ConsumerType,
		Method:       record.Method,
		Payload:      Raw(record.Payload),
		Channels:     record.Channels,
		Error:        record.Error,
		Attempts:     uint32(record.Attempts),
		Status:       string(record.Status),
		CreatedAt:    record.CreatedAt.UnixMilli(),
		UpdatedAt:    record.UpdatedAt.UnixMilli(),
	}
}

// ConsumerQuarantineList returns the failed consumer events currently kept in
// the quarantine area.
func (h *Executor) ConsumerQuarantineList(ctx context.Context, cmd *ConsumerQuarantineListRequest) *ConsumerQuarantineListResponse {
	defer metrics.ObserveAPICommand(time.Now(), h.config.Protocol, "consumer_quarantine_list")

	resp := &ConsumerQuarantineListResponse{}
	if h.quarantine == nil {
		resp.Error = ErrorNotAvailable
		return resp
	}

	records, nextCursor, err := h.quarantine.List(ctx, quarantine.ListFilter{
		Consumer: cmd.Consumer,
		Method:   cmd.Method,
		Limit:    int(cmd.Limit),
		Cursor:   cmd.Cursor,
	})
	if err != nil {
		log.Error().Err(err).Msg("error listing consumer quarantine records")
		resp.Error = ErrorInternal
		return resp
	}

	items := make([]*ConsumerQuarantineRecord, 0, len(records))
	for _, record := range records {
		items = append(items, toAPIQuarantineRecord(record))
	}
	resp.Result = &ConsumerQuarantineListResult{
		Records:    items,
		NextCursor: nextCursor,
		HasMore:    nextCursor != "",
	}
	return resp
}

// ConsumerQuarantineReplay re-dispatches the selected quarantined events. Only
// records replayed successfully are removed; still failing records keep their
// quarantined status with the last error, and unselected records are never
// touched.
func (h *Executor) ConsumerQuarantineReplay(ctx context.Context, cmd *ConsumerQuarantineReplayRequest) *ConsumerQuarantineReplayResponse {
	defer metrics.ObserveAPICommand(time.Now(), h.config.Protocol, "consumer_quarantine_replay")

	resp := &ConsumerQuarantineReplayResponse{}
	if h.quarantine == nil {
		resp.Error = ErrorNotAvailable
		return resp
	}

	report, err := h.quarantine.Replay(ctx, cmd.Ids, quarantine.ListFilter{Consumer: cmd.Consumer})
	if err != nil {
		log.Error().Err(err).Msg("error replaying consumer quarantine records")
		resp.Error = quarantineError(err)
		return resp
	}

	items := make([]*ConsumerQuarantineReplayItem, 0, len(report.Items))
	for _, item := range report.Items {
		items = append(items, &ConsumerQuarantineReplayItem{
			Id:     item.ID,
			Status: string(item.Status),
			Error:  item.Error,
		})
		switch item.Status {
		case quarantine.ItemReplayed:
			metrics.IncConsumerQuarantine(item.Consumer, "replayed")
		case quarantine.ItemFailed:
			metrics.IncConsumerQuarantine(item.Consumer, "failed")
		}
	}
	resp.Result = &ConsumerQuarantineReplayResult{
		Items:    items,
		Replayed: uint32(report.Replayed),
		Failed:   uint32(report.Failed),
		NotFound: uint32(report.NotFound),
	}
	return resp
}
