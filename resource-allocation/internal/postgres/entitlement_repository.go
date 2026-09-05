package postgres

import (
	"context"
	"errors"
	"fmt"

	"cs-ra/internal/resource"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type entitlementRepository struct {
	tx pgx.Tx
}

func newEntitlementRepository(
	tx pgx.Tx,
) resource.Repository {
	return &entitlementRepository{
		tx: tx,
	}
}

func (r *entitlementRepository) ClaimEvent(
	ctx context.Context,
	eventID uuid.UUID,
	eventType string,
	discipleID uuid.UUID,
) (bool, error) {
	const query = `
		INSERT INTO resource_allocation.processed_events (
			event_id,
			event_type,
			disciple_id,
			processed_at
		)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (event_id) DO NOTHING
	`

	result, err := r.tx.Exec(
		ctx,
		query,
		eventID,
		eventType,
		discipleID,
	)
	if err != nil {
		return false, fmt.Errorf(
			"claim processed event %s: %w",
			eventID,
			err,
		)
	}

	return result.RowsAffected() == 1, nil
}

func (r *entitlementRepository) FindByDiscipleIDForUpdate(
	ctx context.Context,
	discipleID uuid.UUID,
) (*resource.Entitlement, error) {
	const query = `
		SELECT
			disciple_id,
			monthly_spirit_stone_allowance,
			residence_level,
			talisman_credits,
			pill_credits,
			updated_at
		FROM resource_allocation.disciple_resource_entitlements
		WHERE disciple_id = $1
		FOR UPDATE
	`

	var params resource.RestoreParams

	err := r.tx.QueryRow(
		ctx,
		query,
		discipleID,
	).Scan(
		&params.DiscipleID,
		&params.MonthlySpiritStoneAllowance,
		&params.ResidenceLevel,
		&params.TalismanCredits,
		&params.PillCredits,
		&params.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, resource.ErrEntitlementNotFound
	}

	if err != nil {
		return nil, fmt.Errorf(
			"find entitlement for disciple %s: %w",
			discipleID,
			err,
		)
	}

	entitlement, err := resource.Restore(params)
	if err != nil {
		return nil, fmt.Errorf(
			"restore entitlement for disciple %s: %w",
			discipleID,
			err,
		)
	}

	return entitlement, nil
}

func (r *entitlementRepository) Update(
	ctx context.Context,
	entitlement *resource.Entitlement,
) error {
	const query = `
		UPDATE resource_allocation.disciple_resource_entitlements
		SET
			monthly_spirit_stone_allowance = $2,
			residence_level = $3,
			talisman_credits = $4,
			pill_credits = $5,
			updated_at = $6
		WHERE disciple_id = $1
	`

	result, err := r.tx.Exec(
		ctx,
		query,
		entitlement.DiscipleID(),
		entitlement.MonthlySpiritStoneAllowance(),
		entitlement.ResidenceLevel(),
		entitlement.TalismanCredits(),
		entitlement.PillCredits(),
		entitlement.UpdatedAt(),
	)
	if err != nil {
		return fmt.Errorf(
			"update entitlement for disciple %s: %w",
			entitlement.DiscipleID(),
			err,
		)
	}

	if result.RowsAffected() == 0 {
		return resource.ErrEntitlementNotFound
	}

	return nil
}
