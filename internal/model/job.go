package model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type JobStatus string

const (
	StatusQueued     JobStatus = "queued"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusCancelled  JobStatus = "cancelled"
	StatusCleaned    JobStatus = "cleaned"
)

type Job struct {
	ID        string    `gorm:"primaryKey;type:uuid" json:"id"`
	ClientIP  string    `gorm:"type:varchar(50);index" json:"client_ip"`
	URL       string    `gorm:"not null" json:"url"`
	Format    string    `gorm:"default:'best'" json:"format"`
	Status    JobStatus `gorm:"type:varchar(20);default:'queued';index" json:"status"`
	Progress  float64   `gorm:"default:0" json:"progress"`
	Speed     string    `gorm:"type:varchar(50)" json:"speed,omitempty"`
	ETA       string    `gorm:"type:varchar(50)" json:"eta,omitempty"`
	FilePath  string    `gorm:"type:text" json:"file_path,omitempty"`
	FileName  string    `gorm:"type:varchar(255)" json:"file_name,omitempty"`
	FileSize  int64     `gorm:"default:0" json:"file_size,omitempty"`
	Extension string    `gorm:"type:varchar(20)" json:"extension,omitempty"`
	Title     string    `gorm:"type:varchar(255)" json:"title,omitempty"`
	Thumbnail string    `gorm:"type:text" json:"thumbnail,omitempty"`
	Duration  float64   `gorm:"default:0" json:"duration,omitempty"`
	ErrorMsg  string    `gorm:"type:text" json:"error_msg,omitempty"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (j *Job) BeforeCreate(tx *gorm.DB) (err error) {
	if j.ID == "" {
		j.ID = uuid.New().String()
	}
	return nil
}

type JobProgressEvent struct {
	JobID     string    `json:"job_id"`
	Status    JobStatus `json:"status"`
	Progress  float64   `json:"progress"`
	Speed     string    `json:"speed,omitempty"`
	ETA       string    `json:"eta,omitempty"`
	Title     string    `json:"title,omitempty"`
	FileName  string    `json:"file_name,omitempty"`
	Extension string    `json:"extension,omitempty"`
	ErrorMsg  string    `json:"error_msg,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type MetadataResponse struct {
	ID          string      `json:"id"`
	Title       string      `json:"title"`
	Description string      `json:"description,omitempty"`
	Uploader    string      `json:"uploader,omitempty"`
	Duration    float64     `json:"duration,omitempty"`
	Thumbnail   string      `json:"thumbnail,omitempty"`
	Formats     interface{} `json:"formats,omitempty"`
	URL         string      `json:"url"`
	FileName    string      `json:"file_name,omitempty"`
	Extension   string      `json:"extension,omitempty"`
	CachedAt    time.Time   `json:"cached_at,omitempty"`
}

type YtDlpVersionResponse struct {
	CurrentVersion string `json:"current_version"`
	Message        string `json:"message,omitempty"`
	Updated        bool   `json:"updated,omitempty"`
}
