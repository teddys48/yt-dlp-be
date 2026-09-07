package handler

import (
	"log/slog"

	"yt-dlp-be/internal/config"
	"yt-dlp-be/internal/logger"
	"yt-dlp-be/internal/middleware"
	rdb "yt-dlp-be/internal/redis"

	"github.com/gin-gonic/gin"
)

func SetupRouter(cfg *config.Config, h *Handler, redisClient *rdb.RedisClient) *gin.Engine {
	// Disable default Gin logger in favor of structured slog middleware
	r := gin.New()
	r.Use(gin.Recovery())

	// Structured Logging & Request ID Middleware
	r.Use(logger.RequestIDMiddleware())
	r.Use(logger.GinLoggerMiddleware(slog.Default()))

	// CORS middleware
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// Health Check
	r.GET("/health", h.HealthCheck)

	// Downloads directory static file serving
	r.Static("/downloads", cfg.DownloadDir)

	// API v1 routes
	v1 := r.Group("/api/v1")
	{
		// Apply Rate Limiter middleware to resource intensive endpoints
		rateLimited := v1.Group("")
		rateLimited.Use(middleware.RateLimiter(cfg, redisClient))
		{
			rateLimited.POST("/metadata", h.GetMetadata)
			rateLimited.POST("/jobs", h.CreateJob)
		}

		v1.GET("/jobs/:id", h.GetJobStatus)
		v1.POST("/jobs/:id/cancel", h.CancelJob)
		v1.GET("/jobs/:id/progress", h.StreamJobProgress)
	}

	return r
}
