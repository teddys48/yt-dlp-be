package db

import (
	"fmt"
	"log/slog"
	"time"

	"yt-dlp-be/internal/config"
	"yt-dlp-be/internal/model"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func InitDB(cfg *config.Config) (*gorm.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName, cfg.DBSSLMode,
	)

	// Configure GORM logger level based on app config
	gormLogLevel := logger.Warn
	if cfg.LogLevel == "debug" {
		gormLogLevel = logger.Info
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(gormLogLevel),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	// Connection pooling configuration
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// Run auto migrations
	if err := db.AutoMigrate(&model.Job{}); err != nil {
		return nil, fmt.Errorf("failed to run database migration: %w", err)
	}

	slog.Info("Successfully connected to PostgreSQL and migrated tables.", slog.String("db_name", cfg.DBName), slog.String("host", cfg.DBHost))
	return db, nil
}
