package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"cs-dr/internal/disciple"

	"github.com/google/uuid"
)

type fakeOutboxRepository struct {
	events             []disciple.CultivationAdvancedEvent
	publishedEventIDs  []uuid.UUID
	failedEvents       []Failure
	markPublishedCalls int
	markFailedCalls    int
}

func (f *fakeOutboxRepository) FetchPending(
	_ context.Context,
	limit int,
) ([]disciple.CultivationAdvancedEvent, error) {
	if len(f.events) > limit {
		return f.events[:limit], nil
	}

	return f.events, nil
}

func (f *fakeOutboxRepository) MarkPublished(
	_ context.Context,
	eventIDs []uuid.UUID,
) error {
	f.markPublishedCalls++
	f.publishedEventIDs = append(
		f.publishedEventIDs,
		eventIDs...,
	)

	return nil
}

func (f *fakeOutboxRepository) MarkFailed(
	_ context.Context,
	failures []Failure,
) error {
	f.markFailedCalls++
	f.failedEvents = append(f.failedEvents, failures...)

	return nil
}

type fakePublisher struct {
	failuresBeforeSuccess map[uuid.UUID]int
	callCounts            map[uuid.UUID]int
	publishedEventIDs     []uuid.UUID
	err                   error
}

func (f *fakePublisher) PublishCultivationAdvanced(
	_ context.Context,
	event disciple.CultivationAdvancedEvent,
) error {
	if f.callCounts == nil {
		f.callCounts = make(map[uuid.UUID]int)
	}

	f.callCounts[event.EventID]++
	if f.callCounts[event.EventID] <=
		f.failuresBeforeSuccess[event.EventID] {
		return f.err
	}

	f.publishedEventIDs = append(
		f.publishedEventIDs,
		event.EventID,
	)

	return nil
}

func TestRelayRunRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		batch    int
		interval time.Duration
		wantErr  string
	}{
		{
			name:     "invalid interval",
			batch:    100,
			interval: 0,
			wantErr:  "outbox: relay interval must be > 0",
		},
		{
			name:     "invalid batch size",
			batch:    0,
			interval: time.Second,
			wantErr:  "outbox: relay batch size must be > 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relay := NewRelay(
				&fakeOutboxRepository{},
				&fakePublisher{},
				testLogger(),
				tt.batch,
				tt.interval,
			)

			err := relay.Run(context.Background())
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("Run() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestRelayBatchWithNoPendingEventsDoesNothing(t *testing.T) {
	t.Parallel()

	repository := &fakeOutboxRepository{}
	publisher := &fakePublisher{}
	relay := NewRelay(
		repository,
		publisher,
		testLogger(),
		100,
		time.Second,
	)

	if err := relay.relayBatch(context.Background()); err != nil {
		t.Fatalf("relayBatch() error = %v", err)
	}

	if len(publisher.publishedEventIDs) != 0 {
		t.Fatalf("published event count = %d, want 0", len(publisher.publishedEventIDs))
	}

	if repository.markPublishedCalls != 0 || repository.markFailedCalls != 0 {
		t.Fatalf(
			"mark calls = (%d, %d), want (0, 0)",
			repository.markPublishedCalls,
			repository.markFailedCalls,
		)
	}
}

func TestRelayBatchPublishesAndMarksSuccessfulEvents(t *testing.T) {
	t.Parallel()

	event := testEvent("00000000-0000-4000-8000-000000000001")
	repository := &fakeOutboxRepository{events: []disciple.CultivationAdvancedEvent{event}}
	publisher := &fakePublisher{}
	relay := NewRelay(
		repository,
		publisher,
		testLogger(),
		100,
		time.Second,
	)

	if err := relay.relayBatch(context.Background()); err != nil {
		t.Fatalf("relayBatch() error = %v", err)
	}

	if publisher.callCounts[event.EventID] != 1 {
		t.Fatalf(
			"publish calls = %d, want 1",
			publisher.callCounts[event.EventID],
		)
	}

	if !sameUUIDs(repository.publishedEventIDs, []uuid.UUID{event.EventID}) {
		t.Fatalf("published IDs = %v, want [%s]", repository.publishedEventIDs, event.EventID)
	}

	if len(repository.failedEvents) != 0 {
		t.Fatalf("failed event count = %d, want 0", len(repository.failedEvents))
	}
}

func TestRelayBatchRetriesAndMarksFailedEvent(t *testing.T) {
	t.Parallel()

	event := testEvent("00000000-0000-4000-8000-000000000002")
	repository := &fakeOutboxRepository{events: []disciple.CultivationAdvancedEvent{event}}
	publisher := &fakePublisher{
		failuresBeforeSuccess: map[uuid.UUID]int{event.EventID: publishAttemptLimit},
		err:                   errors.New("broker unavailable"),
	}
	relay := NewRelay(
		repository,
		publisher,
		testLogger(),
		100,
		time.Second,
	)

	if err := relay.relayBatch(context.Background()); err != nil {
		t.Fatalf("relayBatch() error = %v", err)
	}

	if publisher.callCounts[event.EventID] != publishAttemptLimit {
		t.Fatalf(
			"publish calls = %d, want %d",
			publisher.callCounts[event.EventID],
			publishAttemptLimit,
		)
	}

	if len(repository.failedEvents) != 1 {
		t.Fatalf("failed event count = %d, want 1", len(repository.failedEvents))
	}

	if repository.failedEvents[0].EventID != event.EventID {
		t.Fatalf(
			"failed event ID = %s, want %s",
			repository.failedEvents[0].EventID,
			event.EventID,
		)
	}

	if repository.failedEvents[0].Reason != publisher.err.Error() {
		t.Fatalf(
			"failure reason = %q, want %q",
			repository.failedEvents[0].Reason,
			publisher.err,
		)
	}
}

func TestRelayBatchSeparatesSuccessfulAndFailedEvents(t *testing.T) {
	t.Parallel()

	successfulEvent := testEvent("00000000-0000-4000-8000-000000000003")
	failedEvent := testEvent("00000000-0000-4000-8000-000000000004")
	repository := &fakeOutboxRepository{
		events: []disciple.CultivationAdvancedEvent{
			successfulEvent,
			failedEvent,
		},
	}
	publisher := &fakePublisher{
		failuresBeforeSuccess: map[uuid.UUID]int{
			failedEvent.EventID: publishAttemptLimit,
		},
		err: errors.New("broker unavailable"),
	}
	relay := NewRelay(
		repository,
		publisher,
		testLogger(),
		100,
		time.Second,
	)

	if err := relay.relayBatch(context.Background()); err != nil {
		t.Fatalf("relayBatch() error = %v", err)
	}

	if !sameUUIDs(repository.publishedEventIDs, []uuid.UUID{successfulEvent.EventID}) {
		t.Fatalf("published IDs = %v, want [%s]", repository.publishedEventIDs, successfulEvent.EventID)
	}

	if len(repository.failedEvents) != 1 ||
		repository.failedEvents[0].EventID != failedEvent.EventID {
		t.Fatalf("failed events = %v, want [%s]", repository.failedEvents, failedEvent.EventID)
	}
}

func testEvent(id string) disciple.CultivationAdvancedEvent {
	return disciple.CultivationAdvancedEvent{
		EventID:       uuid.MustParse(id),
		DiscipleID:    uuid.MustParse("00000000-0000-4000-8000-000000000010"),
		PreviousRealm: disciple.RealmSpiritSea,
		PreviousStage: disciple.StageEarly,
		CurrentRealm:  disciple.RealmSpiritSea,
		CurrentStage:  disciple.StageMiddle,
		OccurredAt:    time.Now().UTC(),
	}
}

func sameUUIDs(got, want []uuid.UUID) bool {
	if len(got) != len(want) {
		return false
	}

	for index := range want {
		if got[index] != want[index] {
			return false
		}
	}

	return true
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
