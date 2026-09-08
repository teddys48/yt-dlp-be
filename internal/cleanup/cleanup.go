package cleanup

import (
	"context"
	"log/slog"
	"os"
	"time"

	"yt-dlp-be/internal/config"
	"yt-dlp-be/internal/model"

	"gorm.io/gorm"
)

type CleanupService struct {
	cfg *config.Config
	db  *gorm.DB
}

func NewCleanupService(cfg *config.Config, db *gorm.DB) *CleanupService {
	return &CleanupService{
		cfg: cfg,
		db:  db,
	}
}

// Start launches periodic background file cleanup
func (cs *CleanupService) Start(ctx context.Context) {
	slog.Info("24-Hour File Cleanup Service initialized.",
		slog.Duration("retention", cs.cfg.FileRetentionHours),
		slog.Duration("interval", cs.cfg.CleanupIntervalHour),
	)

	// Run initial cleanup on startup
	cs.runCleanup(ctx)

	ticker := time.NewTicker(cs.cfg.CleanupIntervalHour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cs.runCleanup(ctx)
		case <-ctx.Done():
			slog.Info("Shutting down File Cleanup Service...")
			return
		}
	}
}

func (cs *CleanupService) runCleanup(ctx context.Context) {
	cutoffTime := time.Now().Add(-cs.cfg.FileRetentionHours)

	var expiredJobs []model.Job
	err := cs.db.Where("status = ? AND updated_at < ?", model.StatusCompleted, cutoffTime).Find(&expiredJobs).Error
	if err != nil {
		slog.Error("Failed to query expired jobs for cleanup", slog.String("error", err.Error()))
		return
	}

	if len(expiredJobs) == 0 {
		return
	}

	slog.Info("Starting file cleanup for expired jobs", slog.Int("count", len(expiredJobs)))

	for _, job := range expiredJobs {
		if job.FilePath != "" {
			if err := os.Remove(job.FilePath); err != nil && !os.IsNotExist(err) {
				slog.Warn("Failed to delete expired file from disk",
					slog.String("job_id", job.ID),
					slog.String("file_path", job.FilePath),
					slog.String("error", err.Error()),
				)
			} else {
				slog.Info("Deleted expired file from disk",
					slog.String("job_id", job.ID),
					slog.String("file_name", job.FileName),
				)
			}
		}

		// Update database status to cleaned
		cs.db.Model(&model.Job{}).Where("id = ?", job.ID).Updates(map[string]interface{}{
			"status":    model.StatusCleaned,
			"file_path": "",
		})
	}
}
