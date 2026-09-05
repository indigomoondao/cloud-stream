package resource

import (
	"time"

	"github.com/google/uuid"
)

const CultivationAdvancedEventType = "DISCIPLE_CULTIVATION_ADVANCED"

type CultivationAdvancedEvent struct {
	EventID       uuid.UUID
	DiscipleID    uuid.UUID
	PreviousRealm Realm
	PreviousStage Stage
	CurrentRealm  Realm
	CurrentStage  Stage
	OccurredAt    time.Time
}
