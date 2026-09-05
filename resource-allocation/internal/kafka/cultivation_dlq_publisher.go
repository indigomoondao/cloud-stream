package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type cultivationDLQPublisher struct {
	client kafkaProducer
	topic  string
}

type kafkaProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

func NewCultivationDLQPublisher(
	client kafkaProducer,
	topic string,
) deadLetterPublisher {
	return &cultivationDLQPublisher{
		client: client,
		topic:  topic,
	}
}

type deadLetterEnvelope struct {
	OriginalPayload string    `json:"originalPayload"`
	SourceTopic     string    `json:"sourceTopic"`
	SourcePartition int32     `json:"sourcePartition"`
	SourceOffset    int64     `json:"sourceOffset"`
	Attempts        int       `json:"attempts"`
	Error           string    `json:"error"`
	FailedAt        time.Time `json:"failedAt"`
}

func (p *cultivationDLQPublisher) PublishDeadLetter(
	ctx context.Context,
	record *kgo.Record,
	processingErr error,
	attempts int,
) error {
	if processingErr == nil {
		return errors.New("processing error is required")
	}

	payload, err := json.Marshal(deadLetterEnvelope{
		OriginalPayload: string(record.Value),
		SourceTopic:     record.Topic,
		SourcePartition: record.Partition,
		SourceOffset:    record.Offset,
		Attempts:        attempts,
		Error:           processingErr.Error(),
		FailedAt:        time.Now().UTC(),
	})

	if err != nil {
		return fmt.Errorf("marshal dead letter payload: %w", err)
	}

	dlqRecord := &kgo.Record{
		Topic: p.topic,
		Value: payload,
	}

	if err := p.client.ProduceSync(
		ctx,
		dlqRecord,
	).FirstErr(); err != nil {
		return fmt.Errorf(
			"produce dead letter: %w",
			err,
		)
	}

	return nil
}
