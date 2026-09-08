package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"yt-dlp-be/internal/cleanup"
	"yt-dlp-be/internal/config"
	"yt-dlp-be/internal/db"
	"yt-dlp-be/internal/downloader"
	"yt-dlp-be/internal/logger"
	"yt-dlp-be/internal/queue"
	rdb "yt-dlp-be/internal/redis"
)

func main() {
	cfg := config.LoadConfig()

	// Initialize structured logger
	appLogger := logger.InitLogger(cfg.LogLevel, cfg.LogFormat)
	slog.Info("Starting yt-dlp Worker Process...", slog.String("log_level", cfg.LogLevel))

	// 1. Initialize Database
	database, err := db.InitDB(cfg)
	if err != nil {
		appLogger.Error("Database initialization error", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// 2. Initialize Redis
	redisClient, err := rdb.InitRedis(cfg)
	if err != nil {
		appLogger.Error("Redis initialization error", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// 3. Initialize Downloader & Worker Engine
	dl := downloader.NewDownloader(cfg.DownloadDir)
	worker := queue.NewWorker(cfg, database, redisClient, dl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Start Background 24-Hour File Cleanup Service
	cleanupService := cleanup.NewCleanupService(cfg, database)
	go cleanupService.Start(ctx)

	// Graceful shutdown on signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		slog.Info("Signal received. Shutting down worker queue consumer...")
		cancel()
	}()

	// Start consuming queue jobs
	worker.Start(ctx)
	slog.Info("Worker exited cleanly.")
}
