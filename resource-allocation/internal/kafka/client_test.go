package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewConsumerClientValidatesConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		brokers []string
		topic   string
		groupID string
		wantErr error
	}{
		{
			name:    "missing brokers",
			topic:   "disciple-registry.cultivation-advanced.v1",
			groupID: "resource-allocation-v1",
			wantErr: ErrBrokersRequired,
		},
		{
			name:    "missing topic",
			brokers: []string{"localhost:9092"},
			groupID: "resource-allocation-v1",
			wantErr: ErrTopicRequired,
		},
		{
			name:    "missing consumer group",
			brokers: []string{"localhost:9092"},
			topic:   "disciple-registry.cultivation-advanced.v1",
			wantErr: ErrConsumerGroupRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConsumerClient(
				context.Background(),
				tt.brokers,
				tt.topic,
				tt.groupID,
			)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewConsumerClientReturnsPingError(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()

	_, err := NewConsumerClient(
		ctx,
		[]string{"127.0.0.1:1"},
		"disciple-registry.cultivation-advanced.v1",
		"resource-allocation-v1",
	)
	if err == nil {
		t.Fatal("expected Kafka ping error")
	}
}
