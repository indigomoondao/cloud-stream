package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	client  *kgo.Client
	service *resource.Service
}

func NewCultivationConsumer(
	client *kgo.Client,
	service *resource.Service,
) *cultivationConsumer {
	return &cultivationConsumer{
		client:  client,
		service: service,
	}
}

func (c *cultivationConsumer) Run(
	ctx context.Context,
) error {
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

			if err := c.processRecord(
				ctx,
				record,
			); err != nil {
				return err
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
		log.Printf(
			"cultivation event processed "+
				cultivationEventRecordFormat,
			event.EventID,
			event.DiscipleID,
			record.Partition,
			record.Offset,
		)

		return nil
	}

	log.Printf(
		"cultivation event already processed "+
			cultivationEventRecordFormat,
		event.EventID,
		event.DiscipleID,
		record.Partition,
		record.Offset,
	)

	return nil
}
