package kafka

import (
	"context"
	"testing"
	"time"

	"cs-ra/internal/resource"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerUnitOfWork struct {
	repository resource.Repository
}

func (f consumerUnitOfWork) WithinTransaction(
	ctx context.Context,
	fn func(resource.Repository) error,
) error {
	return fn(f.repository)
}

type consumerRepository struct {
	claimed     bool
	entitlement *resource.Entitlement
	updateCalls int
}

func (f *consumerRepository) ClaimEvent(
	context.Context,
	uuid.UUID,
	string,
	uuid.UUID,
) (bool, error) {
	return f.claimed, nil
}

func (f *consumerRepository) FindByDiscipleIDForUpdate(
	context.Context,
	uuid.UUID,
) (*resource.Entitlement, error) {
	return f.entitlement, nil
}

func (f *consumerRepository) Update(
	context.Context,
	*resource.Entitlement,
) error {
	f.updateCalls++

	return nil
}

func TestProcessRecordProcessesValidEvent(t *testing.T) {
	discipleID := uuid.New()
	entitlement, err := resource.Restore(resource.RestoreParams{
		DiscipleID:                  discipleID,
		MonthlySpiritStoneAllowance: 30,
		ResidenceLevel:              resource.ResidenceLevelOne,
		TalismanCredits:             15,
		PillCredits:                 80,
		UpdatedAt:                   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create entitlement: %v", err)
	}

	repository := &consumerRepository{
		claimed:     true,
		entitlement: entitlement,
	}
	consumer := &cultivationConsumer{
		service: resource.NewService(
			consumerUnitOfWork{repository: repository},
		),
	}

	record := &kgo.Record{
		Value: []byte(`{
      "eventId":"00000000-0000-4000-8000-000000000001",
      "discipleId":"` + discipleID.String() + `",
      "previousRealm":1,
      "previousStage":2,
      "currentRealm":1,
      "currentStage":3,
      "occurredAt":"2026-09-05T00:00:00Z"
    }`),
	}

	if err := consumer.processRecord(context.Background(), record); err != nil {
		t.Fatalf("process record: %v", err)
	}
	if repository.updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", repository.updateCalls)
	}
}

func TestProcessRecordRejectsMalformedJSON(t *testing.T) {
	consumer := &cultivationConsumer{}

	err := consumer.processRecord(
		context.Background(),
		&kgo.Record{Value: []byte(`{"eventId":`)},
	)
	if err == nil {
		t.Fatal("expected malformed JSON error")
	}
}

func TestProcessRecordAcceptsDuplicateEvent(t *testing.T) {
	discipleID := uuid.New()
	repository := &consumerRepository{claimed: false}
	consumer := &cultivationConsumer{
		service: resource.NewService(
			consumerUnitOfWork{repository: repository},
		),
	}
	record := &kgo.Record{
		Value: []byte(`{
      "eventId":"00000000-0000-4000-8000-000000000002",
      "discipleId":"` + discipleID.String() + `",
      "previousRealm":1,
      "previousStage":2,
      "currentRealm":1,
      "currentStage":3,
      "occurredAt":"2026-09-05T00:00:00Z"
    }`),
	}

	if err := consumer.processRecord(context.Background(), record); err != nil {
		t.Fatalf("process duplicate record: %v", err)
	}
	if repository.updateCalls != 0 {
		t.Fatal("did not expect an entitlement update for a duplicate event")
	}
}
