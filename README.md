# yt-dlp Backend Service (Go + Redis + PostgreSQL + FFmpeg)

A production-ready, high-performance Go backend service for `yt-dlp` media download and processing. Features per-IP download tracking, file serving with original video source titles, 24-hour automatic background file cleanup, yt-dlp version updater, PostgreSQL database (`yt-dlp`), real-time progress streaming (SSE), rate limiting, and SSRF protection.

---

## 🌟 Features

- **Media Downloading & Storage**: Downloads media into `./downloads` with metadata, cover thumbnails, and audio extraction (`opus`, `mp3`, `m4a`, `flac`, `mp4`, `mkv`, `webm`).
- **File Download Serving**: Serves downloaded files to clients (`GET /api/v1/jobs/:id/file`) with `Content-Disposition` matching the original source video title (e.g. `Rick Astley - Never Gonna Give You Up.mp4`).
- **Per-IP Download Tracking**: Automatically logs requesting client IP and provides `GET /api/v1/my-downloads` endpoint to query download history per IP address.
- **Automatic 24-Hour File Cleanup**: Background service periodically scans and deletes local media files older than 24 hours, marking database job status as `cleaned`.
- **yt-dlp Version Management**: Built-in endpoints to query current `yt-dlp` version and trigger self-updates (`yt-dlp -U`).
- **Decoupled Architecture**: Independent **API** (`cmd/api`) and **Worker** (`cmd/worker`) nodes.
- **SSRF Protection**: Strict URL validation and DNS resolution checks blocking private/loopback/cloud metadata IP subnets.
- **Real-Time Progress Tracking**: Streams progress updates live to clients via **Server-Sent Events (SSE)** powered by Redis Pub/Sub.
- **Rate Limiting**: Sliding window rate limiter middleware backed by Redis.
- **Containerized**: Full `docker-compose.yml` configuration with PostgreSQL, Redis, API, and Worker containers (`ffmpeg`, `python3`, `py3-mutagen`, `nodejs`, `yt-dlp` pre-installed).

---

## ⚙️ Configuration

Environment settings in `.env`:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=yt-dlp
DB_SSLMODE=disable

# Redis Cache Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0
REDIS_TTL_HOURS=24

# Application Configuration
PORT=8080
DOWNLOAD_DIR=./downloads
JOB_TIMEOUT_MINUTES=15
RATE_LIMIT_REQUESTS=10
RATE_LIMIT_WINDOW_SEC=60
CLEANUP_INTERVAL_HOURS=1
FILE_RETENTION_HOURS=24
LOG_LEVEL=info
LOG_FORMAT=text

# yt-dlp Bot Detection & Authentication Settings
YTDLP_COOKIES_PATH=./cookies.txt
YTDLP_EXTRACTOR_ARGS=youtube:player_client=android,web
```

---

## 🚀 Getting Started

### Option 1: Docker Compose (Recommended)

```bash
docker-compose up --build
```

### Option 2: Local Development

```bash
# 1. Run unit tests
go test -v ./...

# 2. Start the API server
go run ./cmd/api

# 3. Start the Worker process
go run ./cmd/worker
```

---

## 📡 API Reference

### 1. Health Check

- **Endpoint**: `GET /health`
- **Response**:

```json
{
  "postgres": "connected",
  "redis": "connected",
  "status": "ok"
}
```

---

### 2. Check yt-dlp Version

- **Endpoint**: `GET /api/v1/yt-dlp/version`
- **Response**:

```json
{
  "current_version": "2026.03.01"
}
```

---

### 3. Update yt-dlp Executable

Triggers yt-dlp self-update (`yt-dlp -U`).

- **Endpoint**: `POST /api/v1/yt-dlp/update`
- **Response**:

```json
{
  "status": "success",
  "message": "yt-dlp updated successfully",
  "previous_version": "2025.12.08",
  "current_version": "2026.03.01",
  "updated": true
}
```

---

### 4. Extract Video Metadata

Extract video metadata (cached in Redis for 24h). Rate limited.

- **Endpoint**: `POST /api/v1/metadata`
- **Request Body**:

```json
{
  "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
}
```

---

### 5. Enqueue Download Job

Enqueue a media download job. Rate limited. Captures client IP.

- **Endpoint**: `POST /api/v1/jobs`
- **Request Body**:

```json
{
  "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  "format": "opus"
}
```

- **Response**:

```json
{
  "message": "Job successfully enqueued",
  "job": {
    "id": "f516a5b6-7c98-4a92-8051-bdc115cfd37e",
    "client_ip": "127.0.0.1",
    "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "format": "opus",
    "status": "queued",
    "created_at": "2026-09-08T08:50:00Z"
  }
}
```

---

### 6. List Downloads per IP

List all download history for the client's IP.

- **Endpoint**: `GET /api/v1/my-downloads` (Optional query param: `?ip=127.0.0.1`)
- **Response**:

```json
{
  "client_ip": "127.0.0.1",
  "total": 1,
  "jobs": [
    {
      "id": "f516a5b6-7c98-4a92-8051-bdc115cfd37e",
      "client_ip": "127.0.0.1",
      "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
      "format": "opus",
      "status": "completed",
      "progress": 100,
      "title": "Rick Astley - Never Gonna Give You Up",
      "file_name": "Rick Astley - Never Gonna Give You Up.opus",
      "extension": "opus",
      "file_size": 3421000,
      "created_at": "2026-09-08T08:50:00Z"
    }
  ]
}
```

---

### 7. Download File (Original Source Title)

Serves the completed media file for download.

- **Endpoint**: `GET /api/v1/jobs/:id/file`
- **Response**: File binary with `Content-Disposition: attachment; filename="Rick Astley - Never Gonna Give You Up.opus"`

---

### 8. Stream Job Progress (SSE)

Receive live SSE progress updates for a download job.

- **Endpoint**: `GET /api/v1/jobs/:id/progress`
- **Response**: `text/event-stream`

```event-stream
event: progress
data: {"job_id":"f516a5b6-7c98-4a92-8051-bdc115cfd37e","status":"processing","progress":45.2,"speed":"3.2MiB/s","eta":"00:10","timestamp":"2026-09-08T08:50:05Z"}
```
