package queue

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"yt-dlp-be/internal/config"
	"yt-dlp-be/internal/downloader"
	"yt-dlp-be/internal/model"
	rdb "yt-dlp-be/internal/redis"

	"gorm.io/gorm"
)

type Producer struct {
	db          *gorm.DB
	redisClient *rdb.RedisClient
}

func NewProducer(db *gorm.DB, redisClient *rdb.RedisClient) *Producer {
	return &Producer{
		db:          db,
		redisClient: redisClient,
	}
}

func (p *Producer) CreateAndEnqueueJob(ctx context.Context, targetURL string, format string) (*model.Job, error) {
	job := &model.Job{
		URL:      targetURL,
		Format:   format,
		Status:   model.StatusQueued,
		Progress: 0,
	}

	if err := p.db.Create(job).Error; err != nil {
		return nil, err
	}

	if err := p.redisClient.EnqueueJob(ctx, job.ID); err != nil {
		p.db.Model(job).Updates(map[string]interface{}{
			"status":    model.StatusFailed,
			"error_msg": "Failed to push job to Redis queue: " + err.Error(),
		})
		return nil, err
	}

	return job, nil
}

type Worker struct {
	cfg         *config.Config
	db          *gorm.DB
	redisClient *rdb.RedisClient
	downloader  *downloader.Downloader
}

func NewWorker(cfg *config.Config, db *gorm.DB, redisClient *rdb.RedisClient, dl *downloader.Downloader) *Worker {
	return &Worker{
		cfg:         cfg,
		db:          db,
		redisClient: redisClient,
		downloader:  dl,
	}
}

func (w *Worker) Start(ctx context.Context) {
	slog.Info("Worker queue listener started. Waiting for jobs...", slog.String("queue", rdb.QueueName))

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutting down worker queue listener...")
			return
		default:
			jobID, err := w.redisClient.DequeueJob(ctx, 5*time.Second)
			if err != nil {
				// BLPop timeout or error
				continue
			}

			if jobID != "" {
				w.processJob(ctx, jobID)
			}
		}
	}
}

func (w *Worker) processJob(parentCtx context.Context, jobID string) {
	slog.Info("Picked up job from queue", slog.String("job_id", jobID))

	var job model.Job
	if err := w.db.First(&job, "id = ?", jobID).Error; err != nil {
		slog.Error("Job not found in database", slog.String("job_id", jobID), slog.String("error", err.Error()))
		return
	}

	// Skip if job was already cancelled
	if job.Status == model.StatusCancelled {
		slog.Warn("Job was pre-cancelled. Skipping execution.", slog.String("job_id", jobID))
		return
	}

	// Setup job execution timeout
	jobCtx, cancelJob := context.WithTimeout(parentCtx, w.cfg.JobTimeoutMinutes)
	defer cancelJob()

	// Setup cancellation listener via Redis Pub/Sub
	cancelSub := w.redisClient.SubscribeCancelSignal(parentCtx, jobID)
	defer cancelSub.Close()

	ctx, cancelExec := context.WithCancel(jobCtx)
	defer cancelExec()

	// Monitor cancellation signal in goroutine
	go func() {
		ch := cancelSub.Channel()
		select {
		case <-ch:
			slog.Warn("Cancellation signal received for job", slog.String("job_id", jobID))
			w.db.Model(&model.Job{}).Where("id = ?", jobID).Updates(map[string]interface{}{
				"status":    model.StatusCancelled,
				"error_msg": "Job was cancelled by user request",
			})
			_ = w.redisClient.PublishProgress(parentCtx, model.JobProgressEvent{
				JobID:     jobID,
				Status:    model.StatusCancelled,
				ErrorMsg:  "Job cancelled",
				Timestamp: time.Now(),
			})
			cancelExec() // Cancel execution context
		case <-ctx.Done():
			// Execution finished normally or timed out
		}
	}()

	// Mark job as processing
	w.db.Model(&job).Updates(map[string]interface{}{
		"status":     model.StatusProcessing,
		"updated_at": time.Now(),
	})
	_ = w.redisClient.PublishProgress(parentCtx, model.JobProgressEvent{
		JobID:     jobID,
		Status:    model.StatusProcessing,
		Progress:  0,
		Timestamp: time.Now(),
	})

	var lastDBUpdate time.Time

	progressCb := func(percent float64, speed string, eta string) {
		event := model.JobProgressEvent{
			JobID:     jobID,
			Status:    model.StatusProcessing,
			Progress:  percent,
			Speed:     speed,
			ETA:       eta,
			Timestamp: time.Now(),
		}

		// Broadcast real-time progress via Redis Pub/Sub
		_ = w.redisClient.PublishProgress(parentCtx, event)

		// Throttle DB updates to once per 2 seconds to reduce DB load
		if time.Since(lastDBUpdate) > 2*time.Second || percent >= 100 {
			w.db.Model(&model.Job{}).Where("id = ?", jobID).Updates(map[string]interface{}{
				"progress": percent,
				"speed":    speed,
				"eta":      eta,
			})
			lastDBUpdate = time.Now()
		}
	}

	filePath, fileName, fileSize, err := w.downloader.Download(ctx, jobID, job.URL, job.Format, progressCb)

	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			slog.Warn("Job execution context cancelled", slog.String("job_id", jobID))
			return
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			slog.Error("Job execution timed out", slog.String("job_id", jobID), slog.Duration("timeout", w.cfg.JobTimeoutMinutes))
			w.db.Model(&model.Job{}).Where("id = ?", jobID).Updates(map[string]interface{}{
				"status":    model.StatusFailed,
				"error_msg": "Job execution timed out after limit",
			})
			_ = w.redisClient.PublishProgress(parentCtx, model.JobProgressEvent{
				JobID:     jobID,
				Status:    model.StatusFailed,
				ErrorMsg:  "Job timeout",
				Timestamp: time.Now(),
			})
			return
		}

		slog.Error("Job execution failed", slog.String("job_id", jobID), slog.String("error", err.Error()))
		w.db.Model(&model.Job{}).Where("id = ?", jobID).Updates(map[string]interface{}{
			"status":    model.StatusFailed,
			"error_msg": err.Error(),
		})
		_ = w.redisClient.PublishProgress(parentCtx, model.JobProgressEvent{
			JobID:     jobID,
			Status:    model.StatusFailed,
			ErrorMsg:  err.Error(),
			Timestamp: time.Now(),
		})
		return
	}

	// Success
	slog.Info("Job completed successfully",
		slog.String("job_id", jobID),
		slog.String("file_name", fileName),
		slog.Int64("file_size", fileSize),
	)
	w.db.Model(&model.Job{}).Where("id = ?", jobID).Updates(map[string]interface{}{
		"status":    model.StatusCompleted,
		"progress":  100.0,
		"file_path": filePath,
		"file_name": fileName,
		"file_size": fileSize,
		"error_msg": "",
		"speed":     "",
		"eta":       "",
	})

	_ = w.redisClient.PublishProgress(parentCtx, model.JobProgressEvent{
		JobID:     jobID,
		Status:    model.StatusCompleted,
		Progress:  100.0,
		Timestamp: time.Now(),
	})
}
