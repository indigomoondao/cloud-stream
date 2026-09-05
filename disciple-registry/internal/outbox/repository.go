package outbox

import (
	"context"
	"cs-dr/internal/disciple"

	"github.com/google/uuid"
)

type Failure struct {
	EventID uuid.UUID
	Reason  string
}

type Repository interface {
	FetchPending(
		ctx context.Context,
		limit int,
	) ([]disciple.CultivationAdvancedEvent, error)

	MarkPublished(
		ctx context.Context,
		eventIDs []uuid.UUID,
	) error

	MarkFailed(
		ctx context.Context,
		failures []Failure,
	) error
}
