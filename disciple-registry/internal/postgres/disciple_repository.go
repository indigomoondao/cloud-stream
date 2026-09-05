package postgres

import (
	"context"
	"errors"
	"fmt"

	"cs-dr/internal/disciple"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type discipleRepository struct {
	pool *pgxpool.Pool
}

func NewDiscipleRepository(
	pool *pgxpool.Pool,
) disciple.Repository {
	return &discipleRepository{
		pool: pool,
	}
}

func (r *discipleRepository) List(
	ctx context.Context,
) ([]*disciple.Disciple, error) {
	const query = `
		SELECT
			id,
			name,
			birthday,
			cultivation_realm,
			cultivation_stage,
			spirit_root,
			background_type,
			background_note,
			joined_at,
			created_at,
			updated_at
		FROM disciple_registry.disciples
		ORDER BY joined_at, id
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query disciples: %w", err)
	}
	defer rows.Close()

	disciples := make([]*disciple.Disciple, 0)

	for rows.Next() {
		var params disciple.RestoreParams

		err := rows.Scan(
			&params.ID,
			&params.Name,
			&params.Birthday,
			&params.CultivationRealm,
			&params.CultivationStage,
			&params.SpiritRoot,
			&params.BackgroundType,
			&params.BackgroundNote,
			&params.JoinedAt,
			&params.CreatedAt,
			&params.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan disciple: %w", err)
		}

		d, err := disciple.Restore(params)
		if err != nil {
			return nil, fmt.Errorf(
				"restore disciple %s: %w",
				params.ID,
				err,
			)
		}

		disciples = append(disciples, d)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate disciples: %w", err)
	}

	return disciples, nil
}

func (r *discipleRepository) FindByID(
	ctx context.Context,
	id uuid.UUID,
) (*disciple.Disciple, error) {
	const query = `
		SELECT
			id,
			name,
			birthday,
			cultivation_realm,
			cultivation_stage,
			spirit_root,
			background_type,
			background_note,
			joined_at,
			created_at,
			updated_at
		FROM disciple_registry.disciples
		WHERE id = $1
	`

	var params disciple.RestoreParams

	err := r.pool.QueryRow(ctx, query, id).Scan(
		&params.ID,
		&params.Name,
		&params.Birthday,
		&params.CultivationRealm,
		&params.CultivationStage,
		&params.SpiritRoot,
		&params.BackgroundType,
		&params.BackgroundNote,
		&params.JoinedAt,
		&params.CreatedAt,
		&params.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, disciple.ErrDiscipleNotFound
	}

	if err != nil {
		return nil, fmt.Errorf(
			"query disciple %s: %w",
			id,
			err,
		)
	}

	d, err := disciple.Restore(params)
	if err != nil {
		return nil, fmt.Errorf(
			"restore disciple %s: %w",
			id,
			err,
		)
	}

	return d, nil
}

func (r *discipleRepository) UpdateCultivation(
	ctx context.Context,
	d *disciple.Disciple,
	currentRealm disciple.Realm,
	currentStage disciple.Stage,
) error {
	const query = `
		UPDATE disciple_registry.disciples
		SET
			cultivation_realm = $2,
			cultivation_stage = $3,
			updated_at = $4
		WHERE id = $1
		  AND cultivation_realm = $5
		  AND cultivation_stage = $6
	`

	result, err := r.pool.Exec(
		ctx,
		query,
		d.ID(),
		d.Realm(),
		d.Stage(),
		d.UpdatedAt(),
		currentRealm,
		currentStage,
	)
	if err != nil {
		return fmt.Errorf(
			"update cultivation for disciple %s: %w",
			d.ID(),
			err,
		)
	}

	if result.RowsAffected() == 0 {
		return disciple.ErrCultivationConflict
	}

	return nil
}
