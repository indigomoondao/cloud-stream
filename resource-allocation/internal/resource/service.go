package resource

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var ErrInvalidEvent = errors.New(
	"invalid cultivation advanced event",
)

type Service struct {
	unitOfWork UnitOfWork
}

func NewService(
	unitOfWork UnitOfWork,
) *Service {
	return &Service{
		unitOfWork: unitOfWork,
	}
}

func (s *Service) HandleCultivationAdvanced(
	ctx context.Context,
	event CultivationAdvancedEvent,
) (processed bool, err error) {
	if event.EventID == uuid.Nil ||
		event.DiscipleID == uuid.Nil {
		return false, ErrInvalidEvent
	}

	processed = false

	err = s.unitOfWork.WithinTransaction(
		ctx,
		func(repository Repository) error {
			claimed, err := repository.ClaimEvent(
				ctx,
				event.EventID,
				CultivationAdvancedEventType,
				event.DiscipleID,
			)
			if err != nil {
				return err
			}

			// The event was already processed.
			// Treat it as success without repeating the work.
			if !claimed {
				return nil
			}

			entitlement, err :=
				repository.FindByDiscipleIDForUpdate(
					ctx,
					event.DiscipleID,
				)
			if err != nil {
				return err
			}

			if err := entitlement.ApplyCultivationAdvance(
				event.PreviousRealm,
				event.PreviousStage,
				event.CurrentRealm,
				event.CurrentStage,
			); err != nil {
				return err
			}

			if err := repository.Update(
				ctx,
				entitlement,
			); err != nil {
				return err
			}

			processed = true

			return nil
		},
	)
	if err != nil {
		return false, err
	}

	return processed, nil
}
