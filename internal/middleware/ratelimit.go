package middleware

import (
	"fmt"
	"net/http"
	"time"

	"yt-dlp-be/internal/config"
	rdb "yt-dlp-be/internal/redis"

	"github.com/gin-gonic/gin"
)

func RateLimiter(cfg *config.Config, redisClient *rdb.RedisClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		clientIP := c.ClientIP()
		key := fmt.Sprintf("ratelimit:%s", clientIP)
		ctx := c.Request.Context()

		window := time.Duration(cfg.RateLimitWindowSec) * time.Second
		limit := int64(cfg.RateLimitRequests)

		// Increment request counter
		count, err := redisClient.Client.Incr(ctx, key).Result()
		if err != nil {
			// If Redis fails, allow request but log warning
			c.Next()
			return
		}

		// Set expiration on first request in window
		if count == 1 {
			redisClient.Client.Expire(ctx, key, window)
		}

		if count > limit {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":       "Rate limit exceeded. Please try again later.",
				"retry_after": cfg.RateLimitWindowSec,
			})
			c.Abort()
			return
		}

		c.Header("X-RateLimit-Limit", fmt.Sprintf("%d", limit))
		c.Header("X-RateLimit-Remaining", fmt.Sprintf("%d", limit-count))

		c.Next()
	}
}
