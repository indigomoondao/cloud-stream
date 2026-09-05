package disciple

import (
	"time"

	"github.com/google/uuid"
)

type CultivationAdvancedEvent struct {
	EventID       uuid.UUID `json:"eventId"`
	DiscipleID    uuid.UUID `json:"discipleId"`
	PreviousRealm Realm     `json:"previousRealm"`
	PreviousStage Stage     `json:"previousStage"`
	CurrentRealm  Realm     `json:"currentRealm"`
	CurrentStage  Stage     `json:"currentStage"`
	OccurredAt    time.Time `json:"occurredAt"`
}
