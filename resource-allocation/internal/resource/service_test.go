package resource

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeUnitOfWork struct {
	repository Repository
	calls      int
}

func (f *fakeUnitOfWork) WithinTransaction(
	ctx context.Context,
	fn func(Repository) error,
) error {
	f.calls++

	return fn(f.repository)
}

type fakeResourceRepository struct {
	claimed     bool
	entitlement *Entitlement
	updateCalls int
	claimCalls  int
	claimError  error
	findError   error
	updateError error
}

func (f *fakeResourceRepository) ClaimEvent(
	context.Context,
	uuid.UUID,
	string,
	uuid.UUID,
) (bool, error) {
	f.claimCalls++
	if f.claimError != nil {
		return false, f.claimError
	}

	return f.claimed, nil
}

func (f *fakeResourceRepository) FindByDiscipleIDForUpdate(
	context.Context,
	uuid.UUID,
) (*Entitlement, error) {
	if f.findError != nil {
		return nil, f.findError
	}

	return f.entitlement, nil
}

func (f *fakeResourceRepository) Update(
	context.Context,
	*Entitlement,
) error {
	f.updateCalls++
	if f.updateError != nil {
		return f.updateError
	}

	return nil
}

func newTestEntitlement(t *testing.T) *Entitlement {
	t.Helper()

	entitlement, err := Restore(RestoreParams{
		DiscipleID:                  uuid.New(),
		MonthlySpiritStoneAllowance: 30,
		ResidenceLevel:              ResidenceLevelOne,
		TalismanCredits:             15,
		PillCredits:                 80,
		UpdatedAt:                   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create test entitlement: %v", err)
	}

	return entitlement
}

func newTestEvent(discipleID uuid.UUID) CultivationAdvancedEvent {
	return CultivationAdvancedEvent{
		EventID:       uuid.New(),
		DiscipleID:    discipleID,
		PreviousRealm: RealmQiRefining,
		PreviousStage: StageMiddle,
		CurrentRealm:  RealmQiRefining,
		CurrentStage:  StageLate,
		OccurredAt:    time.Now().UTC(),
	}
}

func TestServiceProcessesNewEventOnce(t *testing.T) {
	entitlement := newTestEntitlement(t)
	repository := &fakeResourceRepository{
		claimed:     true,
		entitlement: entitlement,
	}
	unitOfWork := &fakeUnitOfWork{repository: repository}
	service := NewService(unitOfWork)

	processed, err := service.HandleCultivationAdvanced(
		context.Background(),
		newTestEvent(entitlement.DiscipleID()),
	)
	if err != nil {
		t.Fatalf("handle event: %v", err)
	}

	if !processed {
		t.Fatal("expected event to be processed")
	}
	if repository.claimCalls != 1 {
		t.Fatalf("claim calls = %d, want 1", repository.claimCalls)
	}
	if repository.updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", repository.updateCalls)
	}
	if entitlement.MonthlySpiritStoneAllowance() != 35 {
		t.Fatalf("allowance = %d, want 35", entitlement.MonthlySpiritStoneAllowance())
	}
}

func TestServiceSkipsDuplicateEvent(t *testing.T) {
	entitlement := newTestEntitlement(t)
	repository := &fakeResourceRepository{
		claimed:     false,
		entitlement: entitlement,
	}
	service := NewService(&fakeUnitOfWork{repository: repository})

	processed, err := service.HandleCultivationAdvanced(
		context.Background(),
		newTestEvent(entitlement.DiscipleID()),
	)
	if err != nil {
		t.Fatalf("handle duplicate event: %v", err)
	}

	if processed {
		t.Fatal("expected duplicate event to be skipped")
	}
	if repository.updateCalls != 0 {
		t.Fatal("did not expect entitlement update for duplicate event")
	}
	if entitlement.MonthlySpiritStoneAllowance() != 30 {
		t.Fatalf("allowance = %d, want 30", entitlement.MonthlySpiritStoneAllowance())
	}
}

func TestServiceRejectsInvalidEvent(t *testing.T) {
	unitOfWork := &fakeUnitOfWork{
		repository: &fakeResourceRepository{},
	}
	service := NewService(unitOfWork)

	processed, err := service.HandleCultivationAdvanced(
		context.Background(),
		CultivationAdvancedEvent{},
	)
	if err != ErrInvalidEvent {
		t.Fatalf("error = %v, want %v", err, ErrInvalidEvent)
	}
	if processed {
		t.Fatal("expected invalid event not to be processed")
	}
	if unitOfWork.calls != 0 {
		t.Fatal("did not expect a transaction for an invalid event")
	}
}

func TestServiceRejectsInvalidCultivationTransition(t *testing.T) {
	entitlement := newTestEntitlement(t)
	repository := &fakeResourceRepository{
		claimed:     true,
		entitlement: entitlement,
	}
	service := NewService(&fakeUnitOfWork{repository: repository})
	event := newTestEvent(entitlement.DiscipleID())
	event.PreviousStage = StageEarly

	processed, err := service.HandleCultivationAdvanced(
		context.Background(),
		event,
	)
	if err != ErrInvalidCultivationTransition {
		t.Fatalf("error = %v, want %v", err, ErrInvalidCultivationTransition)
	}
	if processed || repository.updateCalls != 0 {
		t.Fatal("did not expect an entitlement update")
	}
}

func TestServiceReturnsClaimError(t *testing.T) {
	wantErr := errors.New("claim failed")
	entitlement := newTestEntitlement(t)
	repository := &fakeResourceRepository{
		claimError:  wantErr,
		entitlement: entitlement,
	}
	service := NewService(&fakeUnitOfWork{repository: repository})

	_, err := service.HandleCultivationAdvanced(
		context.Background(),
		newTestEvent(entitlement.DiscipleID()),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestServiceReturnsEntitlementLookupError(t *testing.T) {
	wantErr := errors.New("lookup failed")
	entitlement := newTestEntitlement(t)
	repository := &fakeResourceRepository{
		claimed:   true,
		findError: wantErr,
	}
	service := NewService(&fakeUnitOfWork{repository: repository})

	_, err := service.HandleCultivationAdvanced(
		context.Background(),
		newTestEvent(entitlement.DiscipleID()),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestServiceReturnsEntitlementUpdateError(t *testing.T) {
	wantErr := errors.New("update failed")
	entitlement := newTestEntitlement(t)
	repository := &fakeResourceRepository{
		claimed:     true,
		entitlement: entitlement,
		updateError: wantErr,
	}
	service := NewService(&fakeUnitOfWork{repository: repository})

	_, err := service.HandleCultivationAdvanced(
		context.Background(),
		newTestEvent(entitlement.DiscipleID()),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}
