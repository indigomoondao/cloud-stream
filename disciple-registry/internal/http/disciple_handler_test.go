package http

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cs-dr/internal/disciple"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type handlerRepository struct {
	disciple  *disciple.Disciple
	listError error
	findError error
	saveError error
}

func (r *handlerRepository) List(
	context.Context,
) ([]*disciple.Disciple, error) {
	if r.listError != nil {
		return nil, r.listError
	}

	return []*disciple.Disciple{r.disciple}, nil
}

func (r *handlerRepository) FindByID(
	context.Context,
	uuid.UUID,
) (*disciple.Disciple, error) {
	if r.findError != nil {
		return nil, r.findError
	}

	return r.disciple, nil
}

func (r *handlerRepository) SaveCultivationAdvance(
	context.Context,
	*disciple.Disciple,
	disciple.CultivationAdvancedEvent,
	disciple.Realm,
	disciple.Stage,
) error {
	if r.saveError != nil {
		return r.saveError
	}

	return nil
}

func newTestDisciple(
	t *testing.T,
	realm disciple.Realm,
	stage disciple.Stage,
) *disciple.Disciple {
	t.Helper()

	id := uuid.New()
	d, err := disciple.Restore(disciple.RestoreParams{
		ID:               id,
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

	return d
}

func newTestHandlerWith(
	t *testing.T,
	repository *handlerRepository,
) (*gin.Engine, uuid.UUID) {
	t.Helper()

	service := disciple.NewService(
		repository,
	)

	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterRoutes(router, NewDiscipleHandler(service))

	return router, repository.disciple.ID()
}

func newTestHandler(t *testing.T) (*gin.Engine, uuid.UUID) {
	t.Helper()

	disciple := newTestDisciple(
		t,
		disciple.RealmQiRefining,
		disciple.StageMiddle,
	)

	return newTestHandlerWith(
		t,
		&handlerRepository{disciple: disciple},
	)
}

func TestAdvanceCultivationHandlerReturnsOK(t *testing.T) {
	router, id := newTestHandler(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		"POST",
		"/api/v1/disciples/"+id.String()+"/cultivation/advance",
		strings.NewReader(`{"currentRealm":1,"currentStage":2}`),
	)
	request.Header.Set("Content-Type", "application/json")

	router.ServeHTTP(recorder, request)

	if recorder.Code != 200 {
		t.Fatalf("status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestAdvanceCultivationHandlerRejectsInvalidID(t *testing.T) {
	router, _ := newTestHandler(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		"POST",
		"/api/v1/disciples/not-a-uuid/cultivation/advance",
		strings.NewReader(`{}`),
	)

	router.ServeHTTP(recorder, request)

	if recorder.Code != 400 {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
}

func TestListHandlerReturnsOK(t *testing.T) {
	router, _ := newTestHandler(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/api/v1/disciples", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != 200 {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
}

func TestListHandlerMapsInternalError(t *testing.T) {
	d := newTestDisciple(t, disciple.RealmQiRefining, disciple.StageMiddle)
	router, _ := newTestHandlerWith(
		t,
		&handlerRepository{
			disciple:  d,
			listError: errors.New("database unavailable"),
		},
	)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/api/v1/disciples", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != 500 {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
}

func TestAdvanceCultivationHandlerRejectsInvalidBody(t *testing.T) {
	router, id := newTestHandler(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		"POST",
		"/api/v1/disciples/"+id.String()+"/cultivation/advance",
		strings.NewReader(`{"currentRealm":`),
	)

	router.ServeHTTP(recorder, request)

	if recorder.Code != 400 {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
}

func TestAdvanceCultivationHandlerMapsNotFound(t *testing.T) {
	d := newTestDisciple(t, disciple.RealmQiRefining, disciple.StageMiddle)
	router, id := newTestHandlerWith(
		t,
		&handlerRepository{
			disciple:  d,
			findError: disciple.ErrDiscipleNotFound,
		},
	)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		"POST",
		"/api/v1/disciples/"+id.String()+"/cultivation/advance",
		strings.NewReader(`{"currentRealm":1,"currentStage":2}`),
	)

	router.ServeHTTP(recorder, request)

	if recorder.Code != 404 {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
}

func TestAdvanceCultivationHandlerMapsConflict(t *testing.T) {
	router, id := newTestHandler(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		"POST",
		"/api/v1/disciples/"+id.String()+"/cultivation/advance",
		strings.NewReader(`{"currentRealm":1,"currentStage":1}`),
	)

	router.ServeHTTP(recorder, request)

	if recorder.Code != 409 {
		t.Fatalf("status = %d, want 409", recorder.Code)
	}
}

func TestAdvanceCultivationHandlerMapsCannotAdvance(t *testing.T) {
	d := newTestDisciple(t, disciple.RealmSpiritSea, disciple.StageLate)
	router, id := newTestHandlerWith(
		t,
		&handlerRepository{disciple: d},
	)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		"POST",
		"/api/v1/disciples/"+id.String()+"/cultivation/advance",
		strings.NewReader(`{"currentRealm":5,"currentStage":3}`),
	)

	router.ServeHTTP(recorder, request)

	if recorder.Code != 409 {
		t.Fatalf("status = %d, want 409", recorder.Code)
	}
}

func TestAdvanceCultivationHandlerMapsInternalError(t *testing.T) {
	wantErr := errors.New("database unavailable")
	d := newTestDisciple(t, disciple.RealmQiRefining, disciple.StageMiddle)
	router, id := newTestHandlerWith(
		t,
		&handlerRepository{
			disciple:  d,
			saveError: wantErr,
		},
	)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		"POST",
		"/api/v1/disciples/"+id.String()+"/cultivation/advance",
		strings.NewReader(`{"currentRealm":1,"currentStage":2}`),
	)

	router.ServeHTTP(recorder, request)

	if recorder.Code != 500 {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
}
