package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"cs-dr/internal/disciple"

	"github.com/twmb/franz-go/pkg/kgo"
)

type cultivationPublisher struct {
	client *kgo.Client
	topic  string
}

func NewCultivationPublisher(
	client *kgo.Client,
	topic string,
) disciple.EventPublisher {
	return &cultivationPublisher{
		client: client,
		topic:  topic,
	}
}

func (p *cultivationPublisher) PublishCultivationAdvanced(
	ctx context.Context,
	event disciple.CultivationAdvancedEvent,
) error {
	// 1. Marshal the event to JSON.

	// The payload is a []byte.
	payload, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf(
			"marshal cultivation advanced event: %w", err,
		)
	}

	// 2. Create the Kafka record.
	// 3. Use the disciple ID as the key.
	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(event.DiscipleID.String()),
		Value: payload,
	}

	// 4. Publish synchronously and check the acknowledgment.
	if err := p.client.ProduceSync(
		ctx,
		record,
	).FirstErr(); err != nil {
		return fmt.Errorf(
			"produce cultivation advanced event: %w",
			err,
		)
	}

	return nil
}
