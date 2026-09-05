package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"cs-dr/internal/disciple"

	"github.com/google/uuid"
)

type Relay struct {
	repository Repository
	publisher  disciple.EventPublisher
	logger     *slog.Logger
	batchSize  int
	interval   time.Duration
}

const (
	publishAttemptLimit = 3
	relayComponent      = "outbox-relay"
)

func NewRelay(
	repository Repository,
	publisher disciple.EventPublisher,
	logger *slog.Logger,
	batchSize int,
	interval time.Duration,
) *Relay {
	if logger == nil {
		logger = slog.Default()
	}

	return &Relay{
		repository: repository,
		publisher:  publisher,
		logger:     logger,
		batchSize:  batchSize,
		interval:   interval,
	}
}

func (r *Relay) Run(ctx context.Context) error {
	if r.interval <= 0 {
		return errors.New("outbox: relay interval must be > 0")
	}

	if r.batchSize <= 0 {
		return errors.New("outbox: relay batch size must be > 0")
	}

	r.logger.InfoContext(
		ctx,
		"outbox relay started",
		"component", relayComponent,
		"batch_size", r.batchSize,
		"poll_interval", r.interval.String(),
	)

	defer r.logger.Info(
		"outbox relay stopped",
		"component", relayComponent,
	)

	if err := r.relayBatch(ctx); err != nil {
		r.logBatchError(ctx, err)
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.relayBatch(ctx); err != nil {
				r.logBatchError(ctx, err)
			}
		}
	}
}

func (r *Relay) relayBatch(ctx context.Context) error {
	startedAt := time.Now()

	events, err := r.repository.FetchPending(ctx, r.batchSize)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		return nil
	}

	successfulEventIDs := make([]uuid.UUID, 0, len(events))
	failedEvents := make([]Failure, 0)

	for _, event := range events {
		var lastErr error
		published := false

		for attempt := 1; attempt <= publishAttemptLimit; attempt++ {
			err := r.publisher.PublishCultivationAdvanced(ctx, event)
			if err == nil {
				successfulEventIDs = append(
					successfulEventIDs,
					event.EventID,
				)
				published = true
				break
			}

			lastErr = err
			r.logger.WarnContext(
				ctx,
				"failed to publish outbox event",
				"component", relayComponent,
				"event_id", event.EventID,
				"disciple_id", event.DiscipleID,
				"attempt", attempt,
				"error", err,
			)

			if ctx.Err() != nil {
				return ctx.Err()
			}
		}

		if !published {
			failedEvents = append(failedEvents, Failure{
				EventID: event.EventID,
				Reason:  lastErr.Error(),
			})
		}
	}

	if err := r.repository.MarkPublished(
		ctx,
		successfulEventIDs,
	); err != nil {
		return err
	}

	if err := r.repository.MarkFailed(
		ctx,
		failedEvents,
	); err != nil {
		return err
	}

	r.logger.InfoContext(
		ctx,
		"outbox batch processed",
		"component", relayComponent,
		"fetched", len(events),
		"published", len(successfulEventIDs),
		"failed", len(failedEvents),
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)

	return nil
}

func (r *Relay) logBatchError(ctx context.Context, err error) {
	r.logger.ErrorContext(
		ctx,
		"outbox relay batch failed",
		"component", relayComponent,
		"retry_in", r.interval.String(),
		"error", err,
	)
}
