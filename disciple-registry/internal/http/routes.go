package http

import (
	nethttp "net/http"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(
	router *gin.Engine,
	discipleHandler *discipleHandler,
) {
	router.GET("/health", func(c *gin.Context) {
		c.JSON(nethttp.StatusOK, gin.H{
			"status": "ok",
		})
	})

	api := router.Group("/api/v1")

	disciples := api.Group("/disciples")
	{
		disciples.GET("", discipleHandler.List)

		disciples.POST(
			"/:id/cultivation/advance",
			discipleHandler.AdvanceCultivation,
		)
	}
}
