package postgres

import (
	"context"
	"fmt"

	"cs-dr/internal/disciple"
	"cs-dr/internal/outbox"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type outboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(
	pool *pgxpool.Pool,
) outbox.Repository {
	return &outboxRepository{
		pool: pool,
	}
}

func (r *outboxRepository) FetchPending(
	ctx context.Context,
	limit int,
) ([]disciple.CultivationAdvancedEvent, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("outbox fetch limit must be positive")
	}

	const query = `
		SELECT
			event_id,
			disciple_id,
			previous_realm,
			previous_stage,
			current_realm,
			current_stage,
			occurred_at
		FROM disciple_registry.outbox_events
		WHERE published_at IS NULL
		  AND event_type = $1
		  AND event_version = $2
		ORDER BY created_at, event_id
		LIMIT $3
	`

	rows, err := r.pool.Query(
		ctx,
		query,
		disciple.CultivationAdvancedEventType,
		disciple.CultivationAdvancedEventVersion,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query pending outbox events: %w", err)
	}
	defer rows.Close()

	events := make(
		[]disciple.CultivationAdvancedEvent,
		0,
		limit,
	)

	for rows.Next() {
		var (
			eventID       uuid.UUID
			discipleID    uuid.UUID
			previousRealm int16
			previousStage int16
			currentRealm  int16
			currentStage  int16
		)

		var event disciple.CultivationAdvancedEvent
		if err := rows.Scan(
			&eventID,
			&discipleID,
			&previousRealm,
			&previousStage,
			&currentRealm,
			&currentStage,
			&event.OccurredAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending outbox event: %w", err)
		}

		event.EventID = eventID
		event.DiscipleID = discipleID
		event.PreviousRealm = disciple.Realm(previousRealm)
		event.PreviousStage = disciple.Stage(previousStage)
		event.CurrentRealm = disciple.Realm(currentRealm)
		event.CurrentStage = disciple.Stage(currentStage)

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox events: %w", err)
	}

	return events, nil
}

func (r *outboxRepository) MarkPublished(
	ctx context.Context,
	eventIDs []uuid.UUID,
) error {
	if len(eventIDs) == 0 {
		return nil
	}

	const query = `
		UPDATE disciple_registry.outbox_events
		SET published_at = now()
		WHERE event_id = ANY($1::uuid[])
		  AND published_at IS NULL
	`

	if _, err := r.pool.Exec(ctx, query, eventIDs); err != nil {
		return fmt.Errorf("mark outbox events published: %w", err)
	}

	return nil
}

func (r *outboxRepository) MarkFailed(
	ctx context.Context,
	failures []outbox.Failure,
) error {
	if len(failures) == 0 {
		return nil
	}

	eventIDs := make([]uuid.UUID, 0, len(failures))
	reasons := make([]string, 0, len(failures))
	for _, failure := range failures {
		eventIDs = append(eventIDs, failure.EventID)
		reasons = append(reasons, failure.Reason)
	}

	const query = `
		UPDATE disciple_registry.outbox_events AS event
		SET
			attempts = event.attempts + 1,
			last_error = failure.reason
		FROM unnest(
			$1::uuid[],
			$2::text[]
		) AS failure(event_id, reason)
		WHERE event.event_id = failure.event_id
		  AND event.published_at IS NULL
	`

	if _, err := r.pool.Exec(
		ctx,
		query,
		eventIDs,
		reasons,
	); err != nil {
		return fmt.Errorf("mark outbox events failed: %w", err)
	}

	return nil
}
