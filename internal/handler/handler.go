package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"yt-dlp-be/internal/config"
	"yt-dlp-be/internal/downloader"
	"yt-dlp-be/internal/model"
	"yt-dlp-be/internal/queue"
	rdb "yt-dlp-be/internal/redis"
	"yt-dlp-be/internal/ssrf"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type Handler struct {
	cfg         *config.Config
	db          *gorm.DB
	redisClient *rdb.RedisClient
	producer    *queue.Producer
	downloader  *downloader.Downloader
}

func NewHandler(
	cfg *config.Config,
	db *gorm.DB,
	redisClient *rdb.RedisClient,
	producer *queue.Producer,
	dl *downloader.Downloader,
) *Handler {
	return &Handler{
		cfg:         cfg,
		db:          db,
		redisClient: redisClient,
		producer:    producer,
		downloader:  dl,
	}
}

type MetadataRequest struct {
	URL string `json:"url" binding:"required"`
}

type DownloadRequest struct {
	URL    string `json:"url" binding:"required"`
	Format string `json:"format"`
}

// GetMetadata retrieves video info with SSRF validation and Redis caching
func (h *Handler) GetMetadata(c *gin.Context) {
	var req MetadataRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
		return
	}

	// 1. SSRF Check
	if err := ssrf.ValidateURL(req.URL); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "SSRF Validation Failed: " + err.Error()})
		return
	}

	ctx := c.Request.Context()

	// 2. Redis Cache Lookup
	if cached, err := h.redisClient.GetCachedMetadata(ctx, req.URL); err == nil && cached != nil {
		c.JSON(http.StatusOK, gin.H{
			"source":   "cache",
			"metadata": cached,
		})
		return
	}

	// 3. Extract Metadata via yt-dlp (with 30s timeout)
	ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	meta, err := h.downloader.ExtractMetadata(ctxTimeout, req.URL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to extract metadata: " + err.Error()})
		return
	}

	// 4. Save to Cache
	_ = h.redisClient.SetCachedMetadata(ctx, req.URL, meta)

	c.JSON(http.StatusOK, gin.H{
		"source":   "extracted",
		"metadata": meta,
	})
}

// CreateJob enqueues a new download job
func (h *Handler) CreateJob(c *gin.Context) {
	var req DownloadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
		return
	}

	// SSRF Check
	if err := ssrf.ValidateURL(req.URL); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "SSRF Validation Failed: " + err.Error()})
		return
	}

	job, err := h.producer.CreateAndEnqueueJob(c.Request.Context(), req.URL, req.Format)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create job: " + err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message": "Job successfully enqueued",
		"job":     job,
	})
}

// GetJobStatus retrieves job details from Postgres
func (h *Handler) GetJobStatus(c *gin.Context) {
	jobID := c.Param("id")

	var job model.Job
	if err := h.db.First(&job, "id = ?", jobID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch job: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"job": job})
}

// CancelJob cancels a pending or running job
func (h *Handler) CancelJob(c *gin.Context) {
	jobID := c.Param("id")

	var job model.Job
	if err := h.db.First(&job, "id = ?", jobID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch job"})
		return
	}

	if job.Status == model.StatusCompleted || job.Status == model.StatusFailed || job.Status == model.StatusCancelled {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Cannot cancel job in state '%s'", job.Status)})
		return
	}

	// Update DB Status
	h.db.Model(&job).Updates(map[string]interface{}{
		"status":    model.StatusCancelled,
		"error_msg": "Cancelled by user API request",
	})

	// Publish Cancel signal via Redis
	_ = h.redisClient.PublishCancelSignal(c.Request.Context(), jobID)

	c.JSON(http.StatusOK, gin.H{
		"message": "Job cancellation request sent successfully",
		"job_id":  jobID,
	})
}

// StreamJobProgress provides SSE (Server-Sent Events) progress updates
func (h *Handler) StreamJobProgress(c *gin.Context) {
	jobID := c.Param("id")

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Access-Control-Allow-Origin", "*")

	pubsub := h.redisClient.SubscribeProgress(c.Request.Context(), jobID)
	defer pubsub.Close()

	ch := pubsub.Channel()

	c.Stream(func(w io.Writer) bool {
		select {
		case msg, ok := <-ch:
			if !ok {
				return false
			}
			c.SSEvent("progress", msg.Payload)

			var event model.JobProgressEvent
			if err := json.Unmarshal([]byte(msg.Payload), &event); err == nil {
				if event.Status == model.StatusCompleted || event.Status == model.StatusFailed || event.Status == model.StatusCancelled {
					return false // Close SSE connection on terminal state
				}
			}
			return true
		case <-c.Request.Context().Done():
			return false
		}
	})
}

// HealthCheck checks DB and Redis connectivity
func (h *Handler) HealthCheck(c *gin.Context) {
	sqlDB, dbErr := h.db.DB()
	dbStatus := "connected"
	if dbErr != nil || sqlDB.Ping() != nil {
		dbStatus = "disconnected"
	}

	redisStatus := "connected"
	if err := h.redisClient.Client.Ping(c.Request.Context()).Err(); err != nil {
		redisStatus = "disconnected"
	}

	status := http.StatusOK
	if dbStatus != "connected" || redisStatus != "connected" {
		status = http.StatusServiceUnavailable
	}

	c.JSON(status, gin.H{
		"status":   "ok",
		"postgres": dbStatus,
		"redis":    redisStatus,
	})
}
