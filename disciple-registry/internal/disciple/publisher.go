package disciple

import "context"

type EventPublisher interface {
	PublishCultivationAdvanced(
		ctx context.Context,
		event CultivationAdvancedEvent,
	) error
}
