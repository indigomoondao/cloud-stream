package resource

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var ErrEntitlementNotFound = errors.New(
	"entitlement not found",
)

type Repository interface {
	ClaimEvent(
		ctx context.Context,
		eventID uuid.UUID,
		eventType string,
		discipleID uuid.UUID,
	) (claimed bool, err error)

	FindByDiscipleIDForUpdate(
		ctx context.Context,
		discipleID uuid.UUID,
	) (*Entitlement, error)

	Update(
		ctx context.Context,
		entitlement *Entitlement,
	) error
}

type UnitOfWork interface {
	WithinTransaction(
		ctx context.Context,
		fn func(repository Repository) error,
	) error
}
