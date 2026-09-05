package http

import (
	"errors"
	"net/http"
	"time"

	"cs-dr/internal/disciple"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type discipleHandler struct {
	service *disciple.Service
}

func NewDiscipleHandler(
	service *disciple.Service,
) *discipleHandler {
	return &discipleHandler{
		service: service,
	}
}

// Requests

type advanceCultivationRequest struct {
	CurrentRealm uint8 `json:"currentRealm" binding:"required"`
	CurrentStage uint8 `json:"currentStage" binding:"required"`
}

// Responses

type cultivationResponse struct {
	Realm     uint8  `json:"realm"`
	RealmName string `json:"realmName"`
	Stage     uint8  `json:"stage"`
	StageName string `json:"stageName"`
	Level     string `json:"level"`
}

type discipleResponse struct {
	ID             uuid.UUID           `json:"id"`
	Name           string              `json:"name"`
	Birthday       *time.Time          `json:"birthday"`
	Cultivation    cultivationResponse `json:"cultivation"`
	SpiritRoot     string              `json:"spiritRoot"`
	BackgroundType string              `json:"backgroundType"`
	BackgroundNote *string             `json:"backgroundNote"`
	JoinedAt       time.Time           `json:"joinedAt"`
	CreatedAt      time.Time           `json:"createdAt"`
	UpdatedAt      time.Time           `json:"updatedAt"`
}

type advanceCultivationResponse struct {
	DiscipleID uuid.UUID           `json:"discipleId"`
	Previous   cultivationResponse `json:"previous"`
	Current    cultivationResponse `json:"current"`
}

// Handlers

func (h *discipleHandler) List(c *gin.Context) {
	disciples, err := h.service.List(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to list disciples",
		})
		return
	}

	response := make([]discipleResponse, 0, len(disciples))

	for _, d := range disciples {
		response = append(
			response,
			newDiscipleResponse(d),
		)
	}

	c.JSON(http.StatusOK, response)
}

func (h *discipleHandler) AdvanceCultivation(c *gin.Context) {
	discipleID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid disciple id",
		})
		return
	}

	var request advanceCultivationRequest

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request body",
		})
		return
	}

	currentRealm := disciple.Realm(request.CurrentRealm)
	currentStage := disciple.Stage(request.CurrentStage)

	result, err := h.service.AdvanceCultivation(
		c.Request.Context(),
		discipleID,
		currentRealm,
		currentStage,
	)
	if err != nil {
		h.handleAdvanceCultivationError(c, err)
		return
	}

	c.JSON(http.StatusOK, advanceCultivationResponse{
		DiscipleID: result.Disciple.ID(),
		Previous: newCultivationResponseFromValues(
			result.PreviousRealm,
			result.PreviousStage,
		),
		Current: newCultivationResponse(
			result.Disciple,
		),
	})
}

// Error mapping

func (h *discipleHandler) handleAdvanceCultivationError(
	c *gin.Context,
	err error,
) {
	switch {
	case errors.Is(
		err,
		disciple.ErrInvalidCurrentCultivation,
	):
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid current cultivation",
		})

	case errors.Is(err, disciple.ErrDiscipleNotFound):
		c.JSON(http.StatusNotFound, gin.H{
			"error": "disciple not found",
		})

	case errors.Is(err, disciple.ErrCultivationConflict):
		c.JSON(http.StatusConflict, gin.H{
			"error": "cultivation state has changed",
		})

	case errors.Is(
		err,
		disciple.ErrCannotAdvanceCultivation,
	):
		c.JSON(http.StatusConflict, gin.H{
			"error": "disciple cannot advance cultivation",
		})

	default:
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to advance cultivation",
		})
	}
}

// Response mapping

func newDiscipleResponse(
	d *disciple.Disciple,
) discipleResponse {
	return discipleResponse{
		ID:             d.ID(),
		Name:           d.Name(),
		Birthday:       d.Birthday(),
		Cultivation:    newCultivationResponse(d),
		SpiritRoot:     d.SpiritRoot(),
		BackgroundType: d.BackgroundType(),
		BackgroundNote: d.BackgroundNote(),
		JoinedAt:       d.JoinedAt(),
		CreatedAt:      d.CreatedAt(),
		UpdatedAt:      d.UpdatedAt(),
	}
}

func newCultivationResponse(
	d *disciple.Disciple,
) cultivationResponse {
	return newCultivationResponseFromValues(
		d.Realm(),
		d.Stage(),
	)
}

func newCultivationResponseFromValues(
	realm disciple.Realm,
	stage disciple.Stage,
) cultivationResponse {
	return cultivationResponse{
		Realm:     uint8(realm),
		RealmName: realm.String(),
		Stage:     uint8(stage),
		StageName: stage.String(),
		Level: realm.String() + "_" +
			stage.String(),
	}
}
