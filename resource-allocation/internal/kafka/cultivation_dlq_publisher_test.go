package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type dlqProducer struct {
	records []*kgo.Record
	err     error
}

func (f *dlqProducer) ProduceSync(
	_ context.Context,
	records ...*kgo.Record,
) kgo.ProduceResults {
	f.records = append(f.records, records...)

	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{
			Record: record,
			Err:    f.err,
		}
	}

	return results
}

func TestCultivationDLQPublisherPublishesEnvelope(t *testing.T) {
	producer := &dlqProducer{}
	publisher := NewCultivationDLQPublisher(
		producer,
		"resource-allocation.cultivation-advanced.dlq.v1",
	)

	originalPayload := []byte(`{"eventId":"event-1","discipleId":"disciple-1"}`)
	sourceRecord := &kgo.Record{
		Topic:     "disciple-registry.cultivation-advanced.v1",
		Value:     originalPayload,
		Partition: 2,
		Offset:    15,
	}
	processingErr := errors.New("database unavailable")

	err := publisher.PublishDeadLetter(
		context.Background(),
		sourceRecord,
		processingErr,
		3,
	)
	if err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}

	if len(producer.records) != 1 {
		t.Fatalf("produced records = %d, want 1", len(producer.records))
	}

	dlqRecord := producer.records[0]
	if dlqRecord.Topic != "resource-allocation.cultivation-advanced.dlq.v1" {
		t.Fatalf("topic = %q, want DLQ topic", dlqRecord.Topic)
	}

	var envelope deadLetterEnvelope
	if err := json.Unmarshal(dlqRecord.Value, &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}

	if envelope.OriginalPayload != string(originalPayload) {
		t.Fatalf("original payload = %q, want %q", envelope.OriginalPayload, originalPayload)
	}
	if envelope.SourceTopic != sourceRecord.Topic {
		t.Fatalf("source topic = %q, want %q", envelope.SourceTopic, sourceRecord.Topic)
	}
	if envelope.SourcePartition != sourceRecord.Partition {
		t.Fatalf("source partition = %d, want %d", envelope.SourcePartition, sourceRecord.Partition)
	}
	if envelope.SourceOffset != sourceRecord.Offset {
		t.Fatalf("source offset = %d, want %d", envelope.SourceOffset, sourceRecord.Offset)
	}
	if envelope.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", envelope.Attempts)
	}
	if envelope.Error != processingErr.Error() {
		t.Fatalf("error = %q, want %q", envelope.Error, processingErr.Error())
	}
	if envelope.FailedAt.IsZero() {
		t.Fatal("failed at must be set")
	}
}

func TestCultivationDLQPublisherRejectsNilProcessingError(t *testing.T) {
	producer := &dlqProducer{}
	publisher := NewCultivationDLQPublisher(producer, "dlq.topic")

	err := publisher.PublishDeadLetter(
		context.Background(),
		&kgo.Record{Value: []byte(`payload`)},
		nil,
		3,
	)
	if err == nil {
		t.Fatal("expected nil processing error to be rejected")
	}
	if len(producer.records) != 0 {
		t.Fatalf("produced records = %d, want 0", len(producer.records))
	}
}

func TestCultivationDLQPublisherReturnsProduceError(t *testing.T) {
	produceErr := errors.New("broker unavailable")
	producer := &dlqProducer{err: produceErr}
	publisher := NewCultivationDLQPublisher(producer, "dlq.topic")

	err := publisher.PublishDeadLetter(
		context.Background(),
		&kgo.Record{Value: []byte(`payload`)},
		errors.New("processing failed"),
		3,
	)
	if !errors.Is(err, produceErr) {
		t.Fatalf("error = %v, want wrapped produce error", err)
	}
}
