package disciple

import (
	"time"

	"github.com/google/uuid"
)

const CultivationAdvancedEventType = "DISCIPLE_CULTIVATION_ADVANCED"
const CultivationAdvancedEventVersion int16 = 1

type CultivationAdvancedEvent struct {
	EventID       uuid.UUID `json:"eventId"`
	DiscipleID    uuid.UUID `json:"discipleId"`
	PreviousRealm Realm     `json:"previousRealm"`
	PreviousStage Stage     `json:"previousStage"`
	CurrentRealm  Realm     `json:"currentRealm"`
	CurrentStage  Stage     `json:"currentStage"`
	OccurredAt    time.Time `json:"occurredAt"`
}

func (CultivationAdvancedEvent) Type() string {
	return CultivationAdvancedEventType
}

func (CultivationAdvancedEvent) Version() int16 {
	return CultivationAdvancedEventVersion
}
