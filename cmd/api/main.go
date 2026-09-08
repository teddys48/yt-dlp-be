package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"yt-dlp-be/internal/cleanup"
	"yt-dlp-be/internal/config"
	"yt-dlp-be/internal/db"
	"yt-dlp-be/internal/handler"
	"yt-dlp-be/internal/logger"
	"yt-dlp-be/internal/queue"
	rdb "yt-dlp-be/internal/redis"
)

func main() {
	cfg := config.LoadConfig()

	// Initialize structured logger
	appLogger := logger.InitLogger(cfg.LogLevel, cfg.LogFormat)
	slog.Info("Starting yt-dlp API Service...", slog.String("port", cfg.Port), slog.String("log_level", cfg.LogLevel))

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

	// 3. Initialize Producer & Handler (Zero local yt-dlp dependency in API)
	producer := queue.NewProducer(database, redisClient)
	h := handler.NewHandler(cfg, database, redisClient, producer)

	// 4. Start Background 24-Hour File Cleanup Service
	cleanupService := cleanup.NewCleanupService(cfg, database)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go cleanupService.Start(ctx)

	// 5. Setup Router
	router := handler.SetupRouter(cfg, h, redisClient)

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	// Graceful Shutdown Channel
	go func() {
		slog.Info("API Server listening", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("API Listen error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("Shutting down API server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("Server forced to shutdown", slog.String("error", err.Error()))
	}

	slog.Info("API Server exited cleanly.")
}
