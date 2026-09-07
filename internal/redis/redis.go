package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"yt-dlp-be/internal/config"
	"yt-dlp-be/internal/model"

	"github.com/redis/go-redis/v9"
)

const (
	QueueName          = "yt_dlp_job_queue"
	ProgressChannelPrefix = "job:progress:"
	CancelChannelPrefix   = "job:cancel:"
)

type RedisClient struct {
	Client *redis.Client
	TTL    time.Duration
}

func InitRedis(cfg *config.Config) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	slog.Info("Connected to Redis successfully", slog.String("host", cfg.RedisHost), slog.String("port", cfg.RedisPort))
	return &RedisClient{
		Client: rdb,
		TTL:    cfg.RedisTTLHours,
	}, nil
}

// Queue methods
func (r *RedisClient) EnqueueJob(ctx context.Context, jobID string) error {
	return r.Client.RPush(ctx, QueueName, jobID).Err()
}

func (r *RedisClient) DequeueJob(ctx context.Context, timeout time.Duration) (string, error) {
	res, err := r.Client.BLPop(ctx, timeout, QueueName).Result()
	if err != nil {
		return "", err
	}
	if len(res) < 2 {
		return "", fmt.Errorf("invalid queue payload")
	}
	return res[1], nil
}

// Metadata Caching
func (r *RedisClient) GetCachedMetadata(ctx context.Context, url string) (*model.MetadataResponse, error) {
	key := fmt.Sprintf("meta:%s", url)
	val, err := r.Client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var meta model.MetadataResponse
	if err := json.Unmarshal([]byte(val), &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (r *RedisClient) SetCachedMetadata(ctx context.Context, url string, meta *model.MetadataResponse) error {
	key := fmt.Sprintf("meta:%s", url)
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return r.Client.Set(ctx, key, data, r.TTL).Err()
}

// Pub/Sub Progress Tracking
func (r *RedisClient) PublishProgress(ctx context.Context, event model.JobProgressEvent) error {
	channel := ProgressChannelPrefix + event.JobID
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return r.Client.Publish(ctx, channel, data).Err()
}

func (r *RedisClient) SubscribeProgress(ctx context.Context, jobID string) *redis.PubSub {
	channel := ProgressChannelPrefix + jobID
	return r.Client.Subscribe(ctx, channel)
}

// Cancellation Signal
func (r *RedisClient) PublishCancelSignal(ctx context.Context, jobID string) error {
	channel := CancelChannelPrefix + jobID
	return r.Client.Publish(ctx, channel, "CANCEL").Err()
}

func (r *RedisClient) SubscribeCancelSignal(ctx context.Context, jobID string) *redis.PubSub {
	channel := CancelChannelPrefix + jobID
	return r.Client.Subscribe(ctx, channel)
}
