package disciple

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Realms

type Realm uint8

const (
	RealmQiRefining              Realm = 1
	RealmFoundationEstablishment Realm = 2
	RealmGoldenCore              Realm = 3
	RealmNascentSoul             Realm = 4
	RealmSpiritSea               Realm = 5
)

func (r Realm) Valid() bool {
	return r >= RealmQiRefining && r <= RealmSpiritSea
}

func (r Realm) String() string {
	switch r {
	case RealmQiRefining:
		return "QI_REFINING"
	case RealmFoundationEstablishment:
		return "FOUNDATION_ESTABLISHMENT"
	case RealmGoldenCore:
		return "GOLDEN_CORE"
	case RealmNascentSoul:
		return "NASCENT_SOUL"
	case RealmSpiritSea:
		return "SPIRIT_SEA"
	default:
		return "UNKNOWN"
	}
}

// Stages

type Stage uint8

const (
	StageEarly  Stage = 1
	StageMiddle Stage = 2
	StageLate   Stage = 3
)

func (s Stage) String() string {
	switch s {
	case StageEarly:
		return "EARLY"
	case StageMiddle:
		return "MIDDLE"
	case StageLate:
		return "LATE"
	default:
		return "UNKNOWN"
	}
}

func (s Stage) Valid() bool {
	return s >= StageEarly && s <= StageLate
}

// Disciple

type Disciple struct {
	id               uuid.UUID
	name             string
	birthday         *time.Time
	cultivationRealm Realm
	cultivationStage Stage
	spiritRoot       string
	backgroundType   string
	backgroundNote   *string
	joinedAt         time.Time
	createdAt        time.Time
	updatedAt        time.Time
}

func (d Disciple) ID() uuid.UUID {
	return d.id
}

func (d Disciple) Name() string {
	return d.name
}

func (d Disciple) Birthday() *time.Time {
	if d.birthday == nil {
		return nil
	}

	birthday := *d.birthday
	return &birthday
}

func (d Disciple) Realm() Realm {
	return d.cultivationRealm
}

func (d Disciple) Stage() Stage {
	return d.cultivationStage
}

func (d Disciple) SpiritRoot() string {
	return d.spiritRoot
}

func (d Disciple) BackgroundType() string {
	return d.backgroundType
}

func (d Disciple) BackgroundNote() *string {
	if d.backgroundNote == nil {
		return nil
	}

	note := *d.backgroundNote
	return &note
}

func (d Disciple) JoinedAt() time.Time {
	return d.joinedAt
}

func (d Disciple) CreatedAt() time.Time {
	return d.createdAt
}

func (d Disciple) UpdatedAt() time.Time {
	return d.updatedAt
}

func (d Disciple) CultivationLevel() string {
	return d.cultivationRealm.String() + "_" +
		d.cultivationStage.String()
}

func (d *Disciple) AdvanceCultivation() error {
	if !d.cultivationRealm.Valid() {
		return ErrInvalidRealm
	}

	if !d.cultivationStage.Valid() {
		return ErrInvalidStage
	}

	if d.cultivationRealm == RealmSpiritSea &&
		d.cultivationStage == StageLate {
		return ErrCannotAdvanceCultivation
	}

	if d.cultivationStage < StageLate {
		d.cultivationStage++
	} else {
		d.cultivationRealm++
		d.cultivationStage = StageEarly
	}

	d.updatedAt = time.Now().UTC()

	return nil
}

var (
	ErrInvalidDiscipleID        = errors.New("invalid disciple id")
	ErrInvalidRealm             = errors.New("invalid cultivation realm")
	ErrInvalidStage             = errors.New("invalid cultivation stage")
	ErrCannotAdvanceCultivation = errors.New("cannot advance cultivation")
)

type RestoreParams struct {
	ID               uuid.UUID
	Name             string
	Birthday         *time.Time
	CultivationRealm Realm
	CultivationStage Stage
	SpiritRoot       string
	BackgroundType   string
	BackgroundNote   *string
	JoinedAt         time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func Restore(params RestoreParams) (*Disciple, error) {
	if params.ID == uuid.Nil {
		return nil, ErrInvalidDiscipleID
	}

	if !params.CultivationRealm.Valid() {
		return nil, ErrInvalidRealm
	}

	if !params.CultivationStage.Valid() {
		return nil, ErrInvalidStage
	}

	return &Disciple{
		id:               params.ID,
		name:             params.Name,
		birthday:         params.Birthday,
		cultivationRealm: params.CultivationRealm,
		cultivationStage: params.CultivationStage,
		spiritRoot:       params.SpiritRoot,
		backgroundType:   params.BackgroundType,
		backgroundNote:   params.BackgroundNote,
		joinedAt:         params.JoinedAt,
		createdAt:        params.CreatedAt,
		updatedAt:        params.UpdatedAt,
	}, nil
}
