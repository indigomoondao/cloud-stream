package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"cs-ra/internal/resource"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

const cultivationEventRecordFormat = "event_id=%s disciple_id=%s partition=%d offset=%d"

type cultivationAdvancedMessage struct {
	EventID       uuid.UUID `json:"eventId"`
	DiscipleID    uuid.UUID `json:"discipleId"`
	PreviousRealm uint8     `json:"previousRealm"`
	PreviousStage uint8     `json:"previousStage"`
	CurrentRealm  uint8     `json:"currentRealm"`
	CurrentStage  uint8     `json:"currentStage"`
	OccurredAt    time.Time `json:"occurredAt"`
}

type cultivationConsumer struct {
	client       *kgo.Client
	service      *resource.Service
	dlqPublisher deadLetterPublisher

	maxAttempts  int
	retryBackoff time.Duration
}

type deadLetterPublisher interface {
	PublishDeadLetter(
		ctx context.Context,
		record *kgo.Record,
		processingErr error,
		attempts int,
	) error
}

func NewCultivationConsumer(
	client *kgo.Client,
	service *resource.Service,
	dlqPublisher deadLetterPublisher,
	maxAttempts int,
	retryBackoff time.Duration,
) *cultivationConsumer {
	return &cultivationConsumer{
		client:       client,
		service:      service,
		dlqPublisher: dlqPublisher,
		maxAttempts:  maxAttempts,
		retryBackoff: retryBackoff,
	}
}

func (c *cultivationConsumer) Run(
	ctx context.Context,
) error {

	if c.maxAttempts <= 0 {
		return errors.New("kafka: max attempts must be > 0")
	}

	if c.retryBackoff < 0 {
		return errors.New("kafka: retry backoff must be >= 0")
	}

	if c.dlqPublisher == nil {
		return errors.New("kafka: dead-letter publisher is required")
	}

	for {
		fetches := c.client.PollFetches(ctx)

		if ctx.Err() != nil {
			return nil
		}

		if err := fetches.Err(); err != nil {
			return fmt.Errorf(
				"poll cultivation advanced events: %w",
				err,
			)
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			var lastErr error
			consumed := false

			for attempt := 1; attempt <= c.maxAttempts; attempt++ {
				err := c.processRecord(
					ctx,
					record,
				)
				if err == nil {
					consumed = true
					break
				}

				lastErr = err

				if attempt < c.maxAttempts {
					slog.Warn(
						"cultivation event processing failed; retrying",
						"topic", record.Topic,
						"partition", record.Partition,
						"offset", record.Offset,
						"attempt", attempt,
						"max_attempts", c.maxAttempts,
						"retry_in", c.retryBackoff,
						"error", err,
					)

					timer := time.NewTimer(c.retryBackoff)

					select {
					case <-timer.C:
					case <-ctx.Done():
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}

						return ctx.Err()
					}
				}
			}

			if !consumed {
				if err := c.dlqPublisher.PublishDeadLetter(
					ctx,
					record,
					lastErr,
					c.maxAttempts,
				); err != nil {
					slog.Error(
						"failed to publish cultivation event to dead letter",
						"topic", record.Topic,
						"partition", record.Partition,
						"offset", record.Offset,
						"attempts", c.maxAttempts,
						"processing_error", lastErr,
						"error", err,
					)

					return fmt.Errorf("publish dead letter: %w", err)
				}

				slog.Warn(
					"cultivation event moved to dead letter",
					"topic", record.Topic,
					"partition", record.Partition,
					"offset", record.Offset,
					"attempts", c.maxAttempts,
					"error", lastErr,
				)
			}

			if err := c.client.CommitRecords(
				ctx,
				record,
			); err != nil {
				return fmt.Errorf(
					"commit cultivation advanced event: %w",
					err,
				)
			}
		}
	}
}

func (c *cultivationConsumer) processRecord(
	ctx context.Context,
	record *kgo.Record,
) error {
	var message cultivationAdvancedMessage

	if err := json.Unmarshal(
		record.Value,
		&message,
	); err != nil {
		return fmt.Errorf(
			"unmarshal cultivation advanced event "+
				"partition=%d offset=%d: %w",
			record.Partition,
			record.Offset,
			err,
		)
	}

	event := resource.CultivationAdvancedEvent{
		EventID:       message.EventID,
		DiscipleID:    message.DiscipleID,
		PreviousRealm: resource.Realm(message.PreviousRealm),
		PreviousStage: resource.Stage(message.PreviousStage),
		CurrentRealm:  resource.Realm(message.CurrentRealm),
		CurrentStage:  resource.Stage(message.CurrentStage),
		OccurredAt:    message.OccurredAt,
	}

	processed, err :=
		c.service.HandleCultivationAdvanced(
			ctx,
			event,
		)
	if err != nil {
		return fmt.Errorf(
			"handle cultivation advanced event "+
				cultivationEventRecordFormat+
				": %w",
			event.EventID,
			event.DiscipleID,
			record.Partition,
			record.Offset,
			err,
		)
	}

	if processed {
		slog.Info(
			"cultivation event processed",
			"event_id", event.EventID,
			"disciple_id", event.DiscipleID,
			"partition", record.Partition,
			"offset", record.Offset,
		)

		return nil
	}

	slog.Info(
		"cultivation event already processed",
		"event_id", event.EventID,
		"disciple_id", event.DiscipleID,
		"partition", record.Partition,
		"offset", record.Offset,
	)

	return nil
}
