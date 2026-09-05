package resource

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type ResidenceLevel uint8

const (
	ResidenceLevelOne   ResidenceLevel = 1
	ResidenceLevelTwo   ResidenceLevel = 2
	ResidenceLevelThree ResidenceLevel = 3
	ResidenceLevelFour  ResidenceLevel = 4
	ResidenceLevelFive  ResidenceLevel = 5
)

func (level ResidenceLevel) Valid() bool {
	return level >= ResidenceLevelOne &&
		level <= ResidenceLevelFive
}

type Entitlement struct {
	discipleID                  uuid.UUID
	monthlySpiritStoneAllowance int
	residenceLevel              ResidenceLevel
	talismanCredits             int
	pillCredits                 int
	updatedAt                   time.Time
}

type EntitlementIncrease struct {
	MonthlySpiritStoneAllowance int
	TalismanCredits             int
	PillCredits                 int
	ResidenceLevel              ResidenceLevel
}

// Cultivation

type Realm uint8

const (
	RealmQiRefining              Realm = 1
	RealmFoundationEstablishment Realm = 2
	RealmGoldenCore              Realm = 3
	RealmNascentSoul             Realm = 4
	RealmSpiritSea               Realm = 5
)

func (realm Realm) Valid() bool {
	return realm >= RealmQiRefining &&
		realm <= RealmSpiritSea
}

type Stage uint8

const (
	StageEarly  Stage = 1
	StageMiddle Stage = 2
	StageLate   Stage = 3
)

func (stage Stage) Valid() bool {
	return stage >= StageEarly &&
		stage <= StageLate
}

// Entitlement policies

var minorEntitlementIncreaseByRealm = map[Realm]EntitlementIncrease{
	RealmQiRefining: {
		MonthlySpiritStoneAllowance: 5,
		TalismanCredits:             2,
		PillCredits:                 10,
	},
	RealmFoundationEstablishment: {
		MonthlySpiritStoneAllowance: 10,
		TalismanCredits:             5,
		PillCredits:                 25,
	},
	RealmGoldenCore: {
		MonthlySpiritStoneAllowance: 20,
		TalismanCredits:             10,
		PillCredits:                 50,
	},
	RealmNascentSoul: {
		MonthlySpiritStoneAllowance: 35,
		TalismanCredits:             20,
		PillCredits:                 80,
	},
	RealmSpiritSea: {
		MonthlySpiritStoneAllowance: 50,
		TalismanCredits:             30,
		PillCredits:                 120,
	},
}

var majorEntitlementIncreaseByRealm = map[Realm]EntitlementIncrease{
	RealmFoundationEstablishment: {
		MonthlySpiritStoneAllowance: 30,
		TalismanCredits:             15,
		PillCredits:                 100,
		ResidenceLevel:              ResidenceLevelTwo,
	},
	RealmGoldenCore: {
		MonthlySpiritStoneAllowance: 60,
		TalismanCredits:             30,
		PillCredits:                 200,
		ResidenceLevel:              ResidenceLevelThree,
	},
	RealmNascentSoul: {
		MonthlySpiritStoneAllowance: 100,
		TalismanCredits:             60,
		PillCredits:                 350,
		ResidenceLevel:              ResidenceLevelFour,
	},
	RealmSpiritSea: {
		MonthlySpiritStoneAllowance: 160,
		TalismanCredits:             100,
		PillCredits:                 550,
		ResidenceLevel:              ResidenceLevelFive,
	},
}

// Errors

var (
	ErrInvalidRealm                 = errors.New("invalid realm")
	ErrInvalidStage                 = errors.New("invalid stage")
	ErrInvalidCultivationTransition = errors.New("invalid cultivation transition")
	ErrEntitlementIncreaseNotFound  = errors.New("entitlement increase not found")
	ErrInvalidDiscipleID            = errors.New("invalid disciple id")
	ErrInvalidEntitlement           = errors.New("invalid entitlement")
	ErrInvalidResidenceLevel        = errors.New(
		"invalid residence level",
	)
)

// Funcs for checking advanced valid

func isMinorCultivationAdvance(
	previousRealm Realm,
	previousStage Stage,
	currentRealm Realm,
	currentStage Stage,
) bool {
	return previousRealm == currentRealm &&
		currentStage == previousStage+1
}

func isMajorCultivationAdvance(
	previousRealm Realm,
	previousStage Stage,
	currentRealm Realm,
	currentStage Stage,
) bool {
	return currentRealm == previousRealm+1 &&
		previousStage == StageLate &&
		currentStage == StageEarly
}

// Get policy

func minorEntitlementIncreaseFor(
	realm Realm,
) (EntitlementIncrease, error) {
	increase, ok :=
		minorEntitlementIncreaseByRealm[realm]
	if !ok {
		return EntitlementIncrease{},
			ErrEntitlementIncreaseNotFound
	}

	return increase, nil
}

func majorEntitlementIncreaseFor(
	realm Realm,
) (EntitlementIncrease, error) {
	increase, ok :=
		majorEntitlementIncreaseByRealm[realm]
	if !ok {
		return EntitlementIncrease{},
			ErrEntitlementIncreaseNotFound
	}

	return increase, nil
}

func (e *Entitlement) applyIncrease(
	increase EntitlementIncrease,
) {
	e.monthlySpiritStoneAllowance +=
		increase.MonthlySpiritStoneAllowance

	e.talismanCredits += increase.TalismanCredits
	e.pillCredits += increase.PillCredits

	if increase.ResidenceLevel > e.residenceLevel {
		e.residenceLevel = increase.ResidenceLevel
	}

	e.updatedAt = time.Now().UTC()
}

func (e *Entitlement) ApplyCultivationAdvance(
	previousRealm Realm,
	previousStage Stage,
	currentRealm Realm,
	currentStage Stage,
) error {
	if !previousRealm.Valid() ||
		!currentRealm.Valid() {
		return ErrInvalidRealm
	}

	if !previousStage.Valid() ||
		!currentStage.Valid() {
		return ErrInvalidStage
	}

	var (
		increase EntitlementIncrease
		err      error
	)

	switch {
	case isMinorCultivationAdvance(
		previousRealm,
		previousStage,
		currentRealm,
		currentStage,
	):
		increase, err =
			minorEntitlementIncreaseFor(currentRealm)

	case isMajorCultivationAdvance(
		previousRealm,
		previousStage,
		currentRealm,
		currentStage,
	):
		increase, err =
			majorEntitlementIncreaseFor(currentRealm)

	default:
		return ErrInvalidCultivationTransition
	}

	if err != nil {
		return err
	}

	e.applyIncrease(increase)

	return nil
}

// Getters

func (e Entitlement) DiscipleID() uuid.UUID {
	return e.discipleID
}

func (e Entitlement) MonthlySpiritStoneAllowance() int {
	return e.monthlySpiritStoneAllowance
}

func (e Entitlement) ResidenceLevel() ResidenceLevel {
	return e.residenceLevel
}

func (e Entitlement) TalismanCredits() int {
	return e.talismanCredits
}

func (e Entitlement) PillCredits() int {
	return e.pillCredits
}

func (e Entitlement) UpdatedAt() time.Time {
	return e.updatedAt
}

type RestoreParams struct {
	DiscipleID                  uuid.UUID
	MonthlySpiritStoneAllowance int
	ResidenceLevel              ResidenceLevel
	TalismanCredits             int
	PillCredits                 int
	UpdatedAt                   time.Time
}

func Restore(params RestoreParams) (*Entitlement, error) {
	if params.DiscipleID == uuid.Nil {
		return nil, ErrInvalidDiscipleID
	}

	if !params.ResidenceLevel.Valid() {
		return nil, ErrInvalidResidenceLevel
	}

	if params.MonthlySpiritStoneAllowance < 0 ||
		params.TalismanCredits < 0 ||
		params.PillCredits < 0 {
		return nil, ErrInvalidEntitlement
	}

	return &Entitlement{
		discipleID: params.DiscipleID,
		monthlySpiritStoneAllowance: params.
			MonthlySpiritStoneAllowance,
		residenceLevel:  params.ResidenceLevel,
		talismanCredits: params.TalismanCredits,
		pillCredits:     params.PillCredits,
		updatedAt:       params.UpdatedAt,
	}, nil
}
