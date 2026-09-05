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

func (r *discipleRepository) SaveCultivationAdvance(
	ctx context.Context,
	d *disciple.Disciple,
	event disciple.CultivationAdvancedEvent,
	expectedRealm disciple.Realm,
	expectedStage disciple.Stage,
) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"begin cultivation advance transaction: %w",
			err,
		)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := r.updateCultivation(
		ctx,
		tx,
		d,
		expectedRealm,
		expectedStage,
	); err != nil {
		return err
	}

	if err := r.insertOutboxEvent(
		ctx,
		tx,
		event,
	); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit cultivation advance transaction: %w",
			err,
		)
	}

	committed = true

	return nil
}

func (r *discipleRepository) updateCultivation(
	ctx context.Context,
	tx pgx.Tx,
	d *disciple.Disciple,
	expectedRealm disciple.Realm,
	expectedStage disciple.Stage,
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

	result, err := tx.Exec(
		ctx,
		query,
		d.ID(),
		d.Realm(),
		d.Stage(),
		d.UpdatedAt(),
		expectedRealm,
		expectedStage,
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

func (r *discipleRepository) insertOutboxEvent(
	ctx context.Context,
	tx pgx.Tx,
	event disciple.CultivationAdvancedEvent,
) error {
	const query = `
		INSERT INTO disciple_registry.outbox_events (
			event_id,
			disciple_id,
			event_type,
			event_version,
			previous_realm,
			previous_stage,
			current_realm,
			current_stage,
			occurred_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err := tx.Exec(
		ctx,
		query,
		event.EventID,
		event.DiscipleID,
		event.Type(),
		event.Version(),
		int16(event.PreviousRealm),
		int16(event.PreviousStage),
		int16(event.CurrentRealm),
		int16(event.CurrentStage),
		event.OccurredAt,
	)
	if err != nil {
		return fmt.Errorf(
			"insert outbox event %s: %w",
			event.EventID,
			err,
		)
	}

	return nil
}
