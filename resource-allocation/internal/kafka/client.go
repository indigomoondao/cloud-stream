package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrBrokersRequired = errors.New(
		"at least one Kafka broker is required",
	)
	ErrTopicRequired = errors.New(
		"Kafka topic is required",
	)
	ErrConsumerGroupRequired = errors.New(
		"Kafka consumer group is required",
	)
)

func NewConsumerClient(
	ctx context.Context,
	brokers []string,
	topic string,
	groupID string,
) (*kgo.Client, error) {
	if len(brokers) == 0 {
		return nil, ErrBrokersRequired
	}

	if topic == "" {
		return nil, ErrTopicRequired
	}

	if groupID == "" {
		return nil, ErrConsumerGroupRequired
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),

		// New consumer groups start from the beginning of the topic.
		kgo.ConsumeResetOffset(
			kgo.NewOffset().AtStart(),
		),

		// Commit offsets manually after successful processing.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create Kafka consumer client: %w",
			err,
		)
	}

	if err := client.Ping(ctx); err != nil {
		client.Close()

		return nil, fmt.Errorf(
			"ping Kafka: %w",
			err,
		)
	}

	return client, nil
}
