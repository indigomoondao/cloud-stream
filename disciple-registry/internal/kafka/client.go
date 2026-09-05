package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrBrokersRequired = errors.New(
	"at least one Kafka broker is required",
)

func NewClient(
	ctx context.Context,
	brokers []string,
) (*kgo.Client, error) {
	if len(brokers) == 0 {
		return nil, ErrBrokersRequired
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create Kafka client: %w",
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
