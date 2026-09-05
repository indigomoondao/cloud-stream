package disciple

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var ErrInvalidCurrentCultivation = errors.New(
	"invalid current cultivation",
)

type AdvanceCultivationResult struct {
	Disciple      *Disciple
	PreviousRealm Realm
	PreviousStage Stage
}

type Service struct {
	repository Repository
}

func NewService(repository Repository) *Service {
	return &Service{
		repository: repository,
	}
}

func (s *Service) List(
	ctx context.Context,
) ([]*Disciple, error) {
	return s.repository.List(ctx)
}

func (s *Service) AdvanceCultivation(
	ctx context.Context,
	discipleID uuid.UUID,
	currentRealm Realm,
	currentStage Stage,
) (*AdvanceCultivationResult, error) {
	if !currentRealm.Valid() ||
		!currentStage.Valid() {
		return nil, ErrInvalidCurrentCultivation
	}

	d, err := s.repository.FindByID(ctx, discipleID)
	if err != nil {
		return nil, err
	}

	if d.Realm() != currentRealm ||
		d.Stage() != currentStage {
		return nil, ErrCultivationConflict
	}

	previousRealm := d.Realm()
	previousStage := d.Stage()

	if err := d.AdvanceCultivation(); err != nil {
		return nil, err
	}

	event := CultivationAdvancedEvent{
		EventID:       uuid.New(),
		DiscipleID:    d.ID(),
		PreviousRealm: previousRealm,
		PreviousStage: previousStage,
		CurrentRealm:  d.Realm(),
		CurrentStage:  d.Stage(),
		OccurredAt:    d.UpdatedAt(),
	}

	if err := s.repository.SaveCultivationAdvance(
		ctx,
		d,
		event,
		currentRealm,
		currentStage,
	); err != nil {
		return nil, err
	}

	return &AdvanceCultivationResult{
		Disciple:      d,
		PreviousRealm: previousRealm,
		PreviousStage: previousStage,
	}, nil
}
