package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type contextKey string

const RequestIDKey contextKey = "request_id"
const RequestIDHeader = "X-Request-ID"

// InitLogger sets up a structured slog logger based on format (json/text) and level (debug/info/warn/error)
func InitLogger(levelStr, formatStr string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: level == slog.LevelDebug,
	}

	var handler slog.Handler
	if strings.ToLower(formatStr) == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// RequestIDMiddleware injects a unique Request ID into headers and context
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.GetHeader(RequestIDHeader)
		if reqID == "" {
			reqID = uuid.New().String()
		}

		c.Header(RequestIDHeader, reqID)
		ctx := context.WithValue(c.Request.Context(), RequestIDKey, reqID)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

// GinLoggerMiddleware logs incoming HTTP request and outgoing response as 2 separate structured log events
func GinLoggerMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		rawQuery := c.Request.URL.RawQuery
		if rawQuery != "" {
			path = path + "?" + rawQuery
		}

		clientIP := c.ClientIP()
		method := c.Request.Method
		userAgent := c.Request.UserAgent()
		reqID, _ := c.Request.Context().Value(RequestIDKey).(string)

		// 1. Log HTTP Request Event (Incoming)
		reqAttrs := []slog.Attr{
			slog.String("request_id", reqID),
			slog.String("method", method),
			slog.String("path", path),
			slog.String("client_ip", clientIP),
		}
		if userAgent != "" {
			reqAttrs = append(reqAttrs, slog.String("user_agent", userAgent))
		}

		logger.LogAttrs(c.Request.Context(), slog.LevelInfo, "HTTP Request", reqAttrs...)

		// Execute downstream handlers
		c.Next()

		// 2. Log HTTP Response Event (Outgoing)
		latency := time.Since(start)
		statusCode := c.Writer.Status()
		bodySize := c.Writer.Size()
		errorMessage := c.Errors.ByType(gin.ErrorTypePrivate).String()

		resAttrs := []slog.Attr{
			slog.String("request_id", reqID),
			slog.String("method", method),
			slog.String("path", path),
			slog.Int("status", statusCode),
			slog.Duration("latency", latency),
			slog.String("client_ip", clientIP),
			slog.Int("size_bytes", bodySize),
		}

		if errorMessage != "" {
			resAttrs = append(resAttrs, slog.String("error", errorMessage))
		}

		msg := "HTTP Response"
		switch {
		case statusCode >= 500:
			logger.LogAttrs(c.Request.Context(), slog.LevelError, msg, resAttrs...)
		case statusCode >= 400:
			logger.LogAttrs(c.Request.Context(), slog.LevelWarn, msg, resAttrs...)
		default:
			logger.LogAttrs(c.Request.Context(), slog.LevelInfo, msg, resAttrs...)
		}
	}
}

// GetRequestID retrieves request ID from context if available
func GetRequestID(ctx context.Context) string {
	if reqID, ok := ctx.Value(RequestIDKey).(string); ok {
		return reqID
	}
	return ""
}
