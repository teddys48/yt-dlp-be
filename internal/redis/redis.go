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
	QueueName             = "yt_dlp_job_queue"
	MetaQueueName         = "yt_dlp_meta_queue"
	ProgressChannelPrefix = "job:progress:"
	CancelChannelPrefix   = "job:cancel:"
	WorkerCmdChannel      = "worker:cmd:channel"
	WorkerCmdResPrefix    = "worker:cmd:res:"
	MetaResPrefix         = "meta:res:"
	YtDlpVersionKey       = "yt_dlp:version"
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

// Metadata Queue methods
type MetaRequestPayload struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type MetaResponsePayload struct {
	ID       string                 `json:"id"`
	Metadata *model.MetadataResponse `json:"metadata,omitempty"`
	Error    string                 `json:"error,omitempty"`
}

func (r *RedisClient) EnqueueMetaRequest(ctx context.Context, payload MetaRequestPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return r.Client.RPush(ctx, MetaQueueName, string(data)).Err()
}

func (r *RedisClient) DequeueMetaRequest(ctx context.Context, timeout time.Duration) (*MetaRequestPayload, error) {
	res, err := r.Client.BLPop(ctx, timeout, MetaQueueName).Result()
	if err != nil {
		return nil, err
	}
	if len(res) < 2 {
		return nil, fmt.Errorf("invalid meta queue payload")
	}
	var payload MetaRequestPayload
	if err := json.Unmarshal([]byte(res[1]), &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (r *RedisClient) PublishMetaResponse(ctx context.Context, payload MetaResponsePayload) error {
	channel := MetaResPrefix + payload.ID
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return r.Client.Publish(ctx, channel, string(data)).Err()
}

func (r *RedisClient) SubscribeMetaResponse(ctx context.Context, requestID string) *redis.PubSub {
	channel := MetaResPrefix + requestID
	return r.Client.Subscribe(ctx, channel)
}

// Worker Command (Version / Update) RPC
type WorkerCmdPayload struct {
	ID     string `json:"id"`
	Action string `json:"action"` // "get_version", "update_ytdlp"
}

type WorkerCmdResponse struct {
	ID         string `json:"id"`
	OldVersion string `json:"old_version,omitempty"`
	NewVersion string `json:"new_version,omitempty"`
	Error      string `json:"error,omitempty"`
}

func (r *RedisClient) PublishWorkerCmd(ctx context.Context, payload WorkerCmdPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return r.Client.Publish(ctx, WorkerCmdChannel, string(data)).Err()
}

func (r *RedisClient) SubscribeWorkerCmd(ctx context.Context) *redis.PubSub {
	return r.Client.Subscribe(ctx, WorkerCmdChannel)
}

func (r *RedisClient) PublishWorkerCmdResponse(ctx context.Context, payload WorkerCmdResponse) error {
	channel := WorkerCmdResPrefix + payload.ID
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return r.Client.Publish(ctx, channel, string(data)).Err()
}

func (r *RedisClient) SubscribeWorkerCmdResponse(ctx context.Context, cmdID string) *redis.PubSub {
	channel := WorkerCmdResPrefix + cmdID
	return r.Client.Subscribe(ctx, channel)
}

// Cached Version helpers
func (r *RedisClient) GetCachedYtDlpVersion(ctx context.Context) (string, error) {
	return r.Client.Get(ctx, YtDlpVersionKey).Result()
}

func (r *RedisClient) SetCachedYtDlpVersion(ctx context.Context, version string) error {
	return r.Client.Set(ctx, YtDlpVersionKey, version, 24*time.Hour).Err()
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
