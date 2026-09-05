package disciple

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var (
	ErrDiscipleNotFound    = errors.New("disciple not found")
	ErrCultivationConflict = errors.New("cultivation state conflict")
)

type Repository interface {
	List(
		ctx context.Context,
	) ([]*Disciple, error)

	FindByID(
		ctx context.Context,
		id uuid.UUID,
	) (*Disciple, error)

	UpdateCultivation(
		ctx context.Context,
		d *Disciple,
		currentRealm Realm,
		currentStage Stage,
	) error
}
