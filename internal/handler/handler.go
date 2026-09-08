package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"yt-dlp-be/internal/config"
	"yt-dlp-be/internal/model"
	"yt-dlp-be/internal/queue"
	rdb "yt-dlp-be/internal/redis"
	"yt-dlp-be/internal/ssrf"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Handler struct {
	cfg         *config.Config
	db          *gorm.DB
	redisClient *rdb.RedisClient
	producer    *queue.Producer
}

func NewHandler(
	cfg *config.Config,
	db *gorm.DB,
	redisClient *rdb.RedisClient,
	producer *queue.Producer,
) *Handler {
	return &Handler{
		cfg:         cfg,
		db:          db,
		redisClient: redisClient,
		producer:    producer,
	}
}

type MetadataRequest struct {
	URL string `json:"url" binding:"required"`
}

type DownloadRequest struct {
	URL      string `json:"url" binding:"required"`
	Format   string `json:"format"`
	ClientIP string `json:"ip"`
	AltIP    string `json:"client_ip"`
}

// GetMetadata retrieves video info via Redis cache or Worker queue RPC
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

	// 3. Delegate Metadata Extraction to Worker Scope via Redis Queue & PubSub RPC
	reqID := uuid.New().String()
	pubsub := h.redisClient.SubscribeMetaResponse(ctx, reqID)
	defer pubsub.Close()

	if err := h.redisClient.EnqueueMetaRequest(ctx, rdb.MetaRequestPayload{ID: reqID, URL: req.URL}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to push metadata request to Worker queue: " + err.Error()})
		return
	}

	ch := pubsub.Channel()
	timeout := time.After(35 * time.Second)

	select {
	case msg, ok := <-ch:
		if !ok {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Worker response channel closed unexpectedly"})
			return
		}

		var payload rdb.MetaResponsePayload
		if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse worker metadata response"})
			return
		}

		if payload.Error != "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Worker metadata extraction failed: " + payload.Error})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"source":   "worker",
			"metadata": payload.Metadata,
		})
		return

	case <-timeout:
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": "Metadata extraction timed out waiting for Worker"})
		return

	case <-ctx.Done():
		c.JSON(http.StatusRequestTimeout, gin.H{"error": "Client cancelled request"})
		return
	}
}

// CreateJob enqueues a new download job with client IP tracking
func (h *Handler) CreateJob(c *gin.Context) {
	var req DownloadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
		return
	}

	// 1. SSRF Check
	if err := ssrf.ValidateURL(req.URL); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "SSRF Validation Failed: " + err.Error()})
		return
	}

	// 2. Resolve Client IP from JSON request body, query params, path param, or request header fallback
	clientIP := resolveIP(c, req.ClientIP, req.AltIP)

	job, err := h.producer.CreateAndEnqueueJob(c.Request.Context(), req.URL, req.Format, clientIP)
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

// GetMyDownloads lists all downloaded files per Client IP
func (h *Handler) GetMyDownloads(c *gin.Context) {
	ip := resolveIP(c, "", "")

	var jobs []model.Job
	if err := h.db.Where("client_ip = ?", ip).Order("created_at DESC").Find(&jobs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch downloads history: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"client_ip": ip,
		"total":     len(jobs),
		"jobs":      jobs,
	})
}

func resolveIP(c *gin.Context, bodyIP, bodyAltIP string) string {
	if ip := strings.TrimSpace(bodyIP); ip != "" {
		return ip
	}
	if ip := strings.TrimSpace(bodyAltIP); ip != "" {
		return ip
	}
	if ip := strings.TrimSpace(c.Param("ip")); ip != "" {
		return ip
	}
	if ip := strings.TrimSpace(c.Query("ip")); ip != "" {
		return ip
	}
	if ip := strings.TrimSpace(c.Query("client_ip")); ip != "" {
		return ip
	}
	return c.ClientIP()
}

// DownloadFile serves the downloaded media file with the original video source title
func (h *Handler) DownloadFile(c *gin.Context) {
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

	if job.Status == model.StatusCleaned {
		c.JSON(http.StatusGone, gin.H{"error": "File was automatically cleaned up after 24 hours retention period"})
		return
	}

	if job.Status != model.StatusCompleted {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Job is in status '%s', file is not ready for download", job.Status)})
		return
	}

	if job.FilePath == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "File path not recorded in database"})
		return
	}

	if _, err := os.Stat(job.FilePath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "File not found on server disk"})
		return
	}

	// Serve file download with original source video filename as attachment
	downloadName := job.FileName
	if downloadName == "" {
		downloadName = fmt.Sprintf("%s.mp4", job.ID)
	}

	c.FileAttachment(job.FilePath, downloadName)
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

	if job.Status == model.StatusCompleted || job.Status == model.StatusFailed || job.Status == model.StatusCancelled || job.Status == model.StatusCleaned {
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
				if event.Status == model.StatusCompleted || event.Status == model.StatusFailed || event.Status == model.StatusCancelled || event.Status == model.StatusCleaned {
					return false // Close SSE connection on terminal state
				}
			}
			return true
		case <-c.Request.Context().Done():
			return false
		}
	})
}

// GetYtDlpVersion retrieves current installed yt-dlp version from Redis cache or Worker RPC
func (h *Handler) GetYtDlpVersion(c *gin.Context) {
	ctx := c.Request.Context()

	// 1. Check Redis Cache
	if ver, err := h.redisClient.GetCachedYtDlpVersion(ctx); err == nil && ver != "" {
		c.JSON(http.StatusOK, model.YtDlpVersionResponse{
			CurrentVersion: ver,
		})
		return
	}

	// 2. Request Version from Worker Scope via Redis PubSub RPC
	cmdID := uuid.New().String()
	pubsub := h.redisClient.SubscribeWorkerCmdResponse(ctx, cmdID)
	defer pubsub.Close()

	if err := h.redisClient.PublishWorkerCmd(ctx, rdb.WorkerCmdPayload{ID: cmdID, Action: "get_version"}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to send version command to Worker: " + err.Error()})
		return
	}

	ch := pubsub.Channel()
	timeout := time.After(10 * time.Second)

	select {
	case msg, ok := <-ch:
		if !ok {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Worker response channel closed unexpectedly"})
			return
		}

		var res rdb.WorkerCmdResponse
		if err := json.Unmarshal([]byte(msg.Payload), &res); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse worker command response"})
			return
		}

		if res.Error != "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Worker version check failed: " + res.Error})
			return
		}

		c.JSON(http.StatusOK, model.YtDlpVersionResponse{
			CurrentVersion: res.NewVersion,
		})
		return

	case <-timeout:
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": "Version check timed out waiting for Worker"})
		return
	}
}

// UpdateYtDlp triggers self-update for yt-dlp inside Worker Scope
func (h *Handler) UpdateYtDlp(c *gin.Context) {
	ctx := c.Request.Context()

	cmdID := uuid.New().String()
	pubsub := h.redisClient.SubscribeWorkerCmdResponse(ctx, cmdID)
	defer pubsub.Close()

	if err := h.redisClient.PublishWorkerCmd(ctx, rdb.WorkerCmdPayload{ID: cmdID, Action: "update_ytdlp"}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to send update command to Worker: " + err.Error()})
		return
	}

	ch := pubsub.Channel()
	timeout := time.After(60 * time.Second)

	select {
	case msg, ok := <-ch:
		if !ok {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Worker response channel closed unexpectedly"})
			return
		}

		var res rdb.WorkerCmdResponse
		if err := json.Unmarshal([]byte(msg.Payload), &res); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse worker update response"})
			return
		}

		if res.Error != "" {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":           "Worker yt-dlp update failed: " + res.Error,
				"current_version": res.OldVersion,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":           "success",
			"message":          "yt-dlp updated successfully",
			"previous_version": res.OldVersion,
			"current_version":  res.NewVersion,
			"updated":          res.OldVersion != res.NewVersion,
		})
		return

	case <-timeout:
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": "yt-dlp update timed out waiting for Worker"})
		return
	}
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
