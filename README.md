# yt-dlp Backend Service (Go + Redis + PostgreSQL + FFmpeg)

A production-ready, highly scalabale backend service for downloading videos via `yt-dlp` and processing media via `ffmpeg`. Built with Go, PostgreSQL, Redis job queues, real-time progress streaming (SSE), rate limiting, and comprehensive SSRF security protection.

---

## 🌟 Key Features

- **Decoupled Architecture**: Independent **API** (`cmd/api`) and **Worker** (`cmd/worker`) services.
- **SSRF Protection**: Strict URL validation, DNS host resolution verification, and blocking of private IPv4/IPv6 ranges, loopbacks, link-local addresses, and cloud metadata IPs (`169.254.169.254`).
- **Real-Time Progress Tracking**: Streams progress updates (percentage, speed, ETA) live to clients via **Server-Sent Events (SSE)** powered by Redis Pub/Sub.
- **Job Queue**: Reliable job queue implemented with Redis (`RPUSH` / `BLPOP`).
- **Metadata Extraction & Caching**: Fast video metadata JSON extraction cached in Redis with configurable TTL (default `24h`).
- **Cancellation & Timeout**: Support for job cancellation via API and hard context timeouts (default `15m`).
- **Rate Limiting**: Sliding window rate limiter middleware backed by Redis.
- **Containerized**: Full `docker-compose.yml` setup including PostgreSQL, Redis, API, and Worker nodes with `yt-dlp` and `ffmpeg` pre-installed.

---

## ⚙️ Configuration

Set the environment variables in `.env` (or let the app use defaults):

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=galau712
DB_NAME=url_shortener
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
```

---

## 🚀 Getting Started

### Option 1: Docker Compose (Recommended)

Run the full stack (PostgreSQL, Redis, API, Worker) with a single command:

```bash
docker-compose up --build
```

### Option 2: Local Development

#### Prerequisites
- Go 1.24+
- PostgreSQL & Redis running
- `yt-dlp` and `ffmpeg` installed on your PATH

#### Steps

1. Run unit tests:
```bash
go test -v ./...
```

2. Start the API server:
```bash
go run ./cmd/api
```

3. Start the Worker process in a separate terminal:
```bash
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

### 2. Extract Metadata
Extract video metadata (cached in Redis for 24h). Rate limited.

- **Endpoint**: `POST /api/v1/metadata`
- **Request Body**:
```json
{
  "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
}
```
- **Response**:
```json
{
  "source": "extracted",
  "metadata": {
    "id": "dQw4w9WgXcQ",
    "title": "Rick Astley - Never Gonna Give You Up",
    "duration": 213,
    "uploader": "RickAstleyVEVO",
    "thumbnail": "https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg",
    "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
  }
}
```

---

### 3. Enqueue Download Job
Enqueue a download task in the Redis queue. Rate limited.

- **Endpoint**: `POST /api/v1/jobs`
- **Request Body**:
```json
{
  "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  "format": "best"
}
```
- **Response**:
```json
{
  "message": "Job successfully enqueued",
  "job": {
    "id": "f516a5b6-7c98-4a92-8051-bdc115cfd37e",
    "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "format": "best",
    "status": "queued",
    "progress": 0,
    "created_at": "2026-09-07T14:40:00Z"
  }
}
```

---

### 4. Stream Job Progress (SSE)
Receive real-time progress events for a download job.

- **Endpoint**: `GET /api/v1/jobs/:id/progress`
- **Response**: `text/event-stream`
```event-stream
event: progress
data: {"job_id":"f516a5b6-7c98-4a92-8051-bdc115cfd37e","status":"processing","progress":45.2,"speed":"3.2MiB/s","eta":"00:10","timestamp":"2026-09-07T14:40:05Z"}
```

---

### 5. Check Job Status
Query current status and output details from PostgreSQL.

- **Endpoint**: `GET /api/v1/jobs/:id`
- **Response**:
```json
{
  "job": {
    "id": "f516a5b6-7c98-4a92-8051-bdc115cfd37e",
    "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    "format": "best",
    "status": "completed",
    "progress": 100,
    "file_path": "./downloads/f516a5b6-7c98-4a92-8051-bdc115cfd37e_Rick_Astley.mp4",
    "file_name": "f516a5b6-7c98-4a92-8051-bdc115cfd37e_Rick_Astley.mp4",
    "file_size": 15423000
  }
}
```

---

### 6. Cancel Job
Cancel a pending or running download job.

- **Endpoint**: `POST /api/v1/jobs/:id/cancel`
- **Response**:
```json
{
  "message": "Job cancellation request sent successfully",
  "job_id": "f516a5b6-7c98-4a92-8051-bdc115cfd37e"
}
```
