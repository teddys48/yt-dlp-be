package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	// DB Config
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string

	// Redis Config
	RedisHost     string
	RedisPort     string
	RedisPassword string
	RedisDB       int
	RedisTTLHours time.Duration

	// Server & Downloader Config
	Port                string
	DownloadDir         string
	JobTimeoutMinutes   time.Duration
	RateLimitRequests   int
	RateLimitWindowSec  int
	CleanupIntervalHour time.Duration
	FileRetentionHours  time.Duration

	// Logger Config
	LogLevel  string
	LogFormat string
}

func LoadConfig() *Config {
	// Load .env if present (ignore error if not exists)
	_ = godotenv.Load()

	return &Config{
		DBHost:              getEnv("DB_HOST", "localhost"),
		DBPort:              getEnv("DB_PORT", "5432"),
		DBUser:              getEnv("DB_USER", "postgres"),
		DBPassword:          getEnv("DB_PASSWORD", "galau712"),
		DBName:              getEnv("DB_NAME", "yt-dlp"),
		DBSSLMode:           getEnv("DB_SSLMODE", "disable"),

		RedisHost:           getEnv("REDIS_HOST", "localhost"),
		RedisPort:           getEnv("REDIS_PORT", "6379"),
		RedisPassword:       getEnv("REDIS_PASSWORD", ""),
		RedisDB:             getEnvAsInt("REDIS_DB", 0),
		RedisTTLHours:       time.Duration(getEnvAsInt("REDIS_TTL_HOURS", 24)) * time.Hour,

		Port:                getEnv("PORT", "8080"),
		DownloadDir:         getEnv("DOWNLOAD_DIR", "./downloads"),
		JobTimeoutMinutes:   time.Duration(getEnvAsInt("JOB_TIMEOUT_MINUTES", 15)) * time.Minute,
		RateLimitRequests:   getEnvAsInt("RATE_LIMIT_REQUESTS", 10),
		RateLimitWindowSec:  getEnvAsInt("RATE_LIMIT_WINDOW_SEC", 60),
		CleanupIntervalHour: time.Duration(getEnvAsInt("CLEANUP_INTERVAL_HOURS", 1)) * time.Hour,
		FileRetentionHours:  time.Duration(getEnvAsInt("FILE_RETENTION_HOURS", 24)) * time.Hour,

		LogLevel:  getEnv("LOG_LEVEL", "info"),
		LogFormat: getEnv("LOG_FORMAT", "text"),
	}
}

func getEnv(key, fallback string) string {
	if val, exists := os.LookupEnv(key); exists && val != "" {
		return val
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	if valStr, exists := os.LookupEnv(key); exists {
		if val, err := strconv.Atoi(valStr); err == nil {
			return val
		}
	}
	return fallback
}
