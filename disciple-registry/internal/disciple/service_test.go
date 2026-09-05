package disciple

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeDiscipleRepository struct {
	disciple     *Disciple
	updateCalled bool
	updateError  error
	findError    error
	updatedRealm Realm
	updatedStage Stage
}

func (f *fakeDiscipleRepository) List(
	context.Context,
) ([]*Disciple, error) {
	return []*Disciple{f.disciple}, nil
}

func (f *fakeDiscipleRepository) FindByID(
	context.Context,
	uuid.UUID,
) (*Disciple, error) {
	if f.findError != nil {
		return nil, f.findError
	}

	return f.disciple, nil
}

func (f *fakeDiscipleRepository) UpdateCultivation(
	context.Context,
	*Disciple,
	Realm,
	Stage,
) error {
	f.updateCalled = true

	if f.updateError != nil {
		return f.updateError
	}

	f.updatedRealm = f.disciple.Realm()
	f.updatedStage = f.disciple.Stage()

	return nil
}

type fakeEventPublisher struct {
	events []CultivationAdvancedEvent
	err    error
}

func (f *fakeEventPublisher) PublishCultivationAdvanced(
	_ context.Context,
	event CultivationAdvancedEvent,
) error {
	f.events = append(f.events, event)

	return f.err
}

func newTestDisciple(t *testing.T, realm Realm, stage Stage) *Disciple {
	t.Helper()

	disciple, err := Restore(RestoreParams{
		ID:               uuid.New(),
		Name:             "Gu Chen",
		CultivationRealm: realm,
		CultivationStage: stage,
		SpiritRoot:       "FIVE_ELEMENTS",
		BackgroundType:   "COMMON",
		JoinedAt:         time.Now().UTC(),
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create test disciple: %v", err)
	}

	return disciple
}

func TestServiceAdvanceCultivationUpdatesAndPublishes(t *testing.T) {
	disciple := newTestDisciple(
		t,
		RealmQiRefining,
		StageMiddle,
	)
	repository := &fakeDiscipleRepository{disciple: disciple}
	publisher := &fakeEventPublisher{}
	service := NewService(repository, publisher)

	result, err := service.AdvanceCultivation(
		context.Background(),
		disciple.ID(),
		RealmQiRefining,
		StageMiddle,
	)
	if err != nil {
		t.Fatalf("advance cultivation: %v", err)
	}

	if result.Disciple.Stage() != StageLate {
		t.Fatalf("stage = %v, want %v", result.Disciple.Stage(), StageLate)
	}

	if !repository.updateCalled {
		t.Fatal("expected repository update")
	}

	if len(publisher.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(publisher.events))
	}

	event := publisher.events[0]
	if event.DiscipleID != disciple.ID() {
		t.Fatalf("event disciple ID = %s, want %s", event.DiscipleID, disciple.ID())
	}
	if event.PreviousStage != StageMiddle || event.CurrentStage != StageLate {
		t.Fatalf("unexpected event stages: %v -> %v", event.PreviousStage, event.CurrentStage)
	}
}

func TestServiceListReturnsDisciples(t *testing.T) {
	disciple := newTestDisciple(
		t,
		RealmQiRefining,
		StageMiddle,
	)
	service := NewService(
		&fakeDiscipleRepository{disciple: disciple},
		&fakeEventPublisher{},
	)

	got, err := service.List(context.Background())
	if err != nil {
		t.Fatalf("list disciples: %v", err)
	}
	if len(got) != 1 || got[0].ID() != disciple.ID() {
		t.Fatalf("unexpected disciples: %v", got)
	}
}

func TestServiceAdvanceCultivationRejectsStaleState(t *testing.T) {
	disciple := newTestDisciple(
		t,
		RealmQiRefining,
		StageMiddle,
	)
	repository := &fakeDiscipleRepository{disciple: disciple}
	publisher := &fakeEventPublisher{}
	service := NewService(repository, publisher)

	_, err := service.AdvanceCultivation(
		context.Background(),
		disciple.ID(),
		RealmQiRefining,
		StageEarly,
	)
	if err != ErrCultivationConflict {
		t.Fatalf("error = %v, want %v", err, ErrCultivationConflict)
	}

	if repository.updateCalled {
		t.Fatal("did not expect repository update")
	}

	if len(publisher.events) != 0 {
		t.Fatal("did not expect an event")
	}
}

func TestServiceAdvanceCultivationRejectsInvalidCurrentState(t *testing.T) {
	disciple := newTestDisciple(
		t,
		RealmQiRefining,
		StageMiddle,
	)
	repository := &fakeDiscipleRepository{disciple: disciple}
	publisher := &fakeEventPublisher{}
	service := NewService(repository, publisher)

	_, err := service.AdvanceCultivation(
		context.Background(),
		disciple.ID(),
		0,
		StageMiddle,
	)
	if err != ErrInvalidCurrentCultivation {
		t.Fatalf("error = %v, want %v", err, ErrInvalidCurrentCultivation)
	}
}

func TestServiceAdvanceCultivationReturnsRepositoryError(t *testing.T) {
	disciple := newTestDisciple(
		t,
		RealmQiRefining,
		StageMiddle,
	)
	wantErr := errors.New("repository unavailable")
	repository := &fakeDiscipleRepository{
		disciple:  disciple,
		findError: wantErr,
	}
	service := NewService(repository, &fakeEventPublisher{})

	_, err := service.AdvanceCultivation(
		context.Background(),
		disciple.ID(),
		RealmQiRefining,
		StageMiddle,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestServiceAdvanceCultivationRejectsMaximumCultivation(t *testing.T) {
	disciple := newTestDisciple(
		t,
		RealmSpiritSea,
		StageLate,
	)
	repository := &fakeDiscipleRepository{disciple: disciple}
	publisher := &fakeEventPublisher{}
	service := NewService(repository, publisher)

	_, err := service.AdvanceCultivation(
		context.Background(),
		disciple.ID(),
		RealmSpiritSea,
		StageLate,
	)
	if err != ErrCannotAdvanceCultivation {
		t.Fatalf("error = %v, want %v", err, ErrCannotAdvanceCultivation)
	}
	if repository.updateCalled || len(publisher.events) != 0 {
		t.Fatal("did not expect persistence or publication")
	}
}

func TestServiceAdvanceCultivationReturnsUpdateError(t *testing.T) {
	disciple := newTestDisciple(
		t,
		RealmQiRefining,
		StageMiddle,
	)
	wantErr := errors.New("update failed")
	repository := &fakeDiscipleRepository{
		disciple:    disciple,
		updateError: wantErr,
	}
	service := NewService(repository, &fakeEventPublisher{})

	_, err := service.AdvanceCultivation(
		context.Background(),
		disciple.ID(),
		RealmQiRefining,
		StageMiddle,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestServiceAdvanceCultivationReturnsPublishError(t *testing.T) {
	disciple := newTestDisciple(
		t,
		RealmQiRefining,
		StageMiddle,
	)
	wantErr := errors.New("publish failed")
	repository := &fakeDiscipleRepository{disciple: disciple}
	publisher := &fakeEventPublisher{err: wantErr}
	service := NewService(repository, publisher)

	_, err := service.AdvanceCultivation(
		context.Background(),
		disciple.ID(),
		RealmQiRefining,
		StageMiddle,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}
