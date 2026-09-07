package downloader

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"yt-dlp-be/internal/model"
)

type ProgressCallback func(percent float64, speed string, eta string)

type Downloader struct {
	DownloadDir string
}

func NewDownloader(downloadDir string) *Downloader {
	_ = os.MkdirAll(downloadDir, 0755)
	return &Downloader{DownloadDir: downloadDir}
}

// ExtractMetadata executes yt-dlp --dump-json
func (d *Downloader) ExtractMetadata(ctx context.Context, targetURL string) (*model.MetadataResponse, error) {
	cmd := exec.CommandContext(ctx, "yt-dlp",
		"--dump-json",
		"--no-playlist",
		"--no-warnings",
		targetURL,
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("yt-dlp metadata extraction failed: %w, stderr: %s", err, stderr.String())
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
		return nil, fmt.Errorf("failed to parse yt-dlp json metadata: %w", err)
	}

	meta := &model.MetadataResponse{
		URL:      targetURL,
		CachedAt: time.Now(),
	}

	if id, ok := raw["id"].(string); ok {
		meta.ID = id
	}
	if title, ok := raw["title"].(string); ok {
		meta.Title = title
	}
	if desc, ok := raw["description"].(string); ok {
		meta.Description = desc
	}
	if uploader, ok := raw["uploader"].(string); ok {
		meta.Uploader = uploader
	}
	if duration, ok := raw["duration"].(float64); ok {
		meta.Duration = duration
	}
	if thumb, ok := raw["thumbnail"].(string); ok {
		meta.Thumbnail = thumb
	}
	if formats, ok := raw["formats"]; ok {
		meta.Formats = formats
	}

	return meta, nil
}

// Download runs yt-dlp download with real-time stdout progress parsing and cancellation/timeout support
func (d *Downloader) Download(
	ctx context.Context,
	jobID string,
	targetURL string,
	format string,
	onProgress ProgressCallback,
) (filePath string, fileName string, fileSize int64, err error) {

	args := buildYtDlpArgs(jobID, targetURL, format, d.DownloadDir)

	cmd := exec.CommandContext(ctx, "yt-dlp", args...)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", "", 0, fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return "", "", 0, fmt.Errorf("failed to start yt-dlp process: %w", err)
	}

	// Read stdout line by line for progress updates
	scanner := bufio.NewScanner(stdoutPipe)
	percentRegex := regexp.MustCompile(`([\d\.]+)%`)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, "|")
			if len(parts) == 3 {
				rawPercent := strings.TrimSpace(parts[0])
				speed := strings.TrimSpace(parts[1])
				eta := strings.TrimSpace(parts[2])

				matches := percentRegex.FindStringSubmatch(rawPercent)
				if len(matches) > 1 {
					if p, err := strconv.ParseFloat(matches[1], 64); err == nil {
						if onProgress != nil {
							onProgress(p, speed, eta)
						}
					}
				}
			}
		}
	}()

	// Wait for completion or context cancellation
	cmdErr := cmd.Wait()
	if ctx.Err() != nil {
		// Context was cancelled or timed out
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return "", "", 0, fmt.Errorf("job download aborted: %w", ctx.Err())
	}

	if cmdErr != nil {
		return "", "", 0, fmt.Errorf("yt-dlp failed: %w, stderr: %s", cmdErr, stderrBuf.String())
	}

	// Find output downloaded file matching pattern jobID_*
	matches, globErr := filepath.Glob(filepath.Join(d.DownloadDir, jobID+"_*"))
	if globErr != nil || len(matches) == 0 {
		return "", "", 0, fmt.Errorf("downloaded file not found on disk after completion")
	}

	finalPath := matches[0]
	fInfo, err := os.Stat(finalPath)
	if err != nil {
		return finalPath, filepath.Base(finalPath), 0, nil
	}

	return finalPath, fInfo.Name(), fInfo.Size(), nil
}

// CopyFile helper for file operations if needed
func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func buildYtDlpArgs(jobID, targetURL, format, downloadDir string) []string {
	outputTemplate := filepath.Join(downloadDir, fmt.Sprintf("%s_%%(title).50s.%%(ext)s", jobID))

	args := []string{
		"--newline",
		"--no-update",
		"--js-runtimes", "node",
		"--add-metadata",
		"--embed-thumbnail",
		"--convert-thumbnails", "jpg",
		"--progress-template", "%(progress._percent_str)s|%(progress._speed_str)s|%(progress._eta_str)s",
		"-o", outputTemplate,
		"--no-playlist",
	}

	normFormat := strings.ToLower(strings.TrimSpace(format))

	// Known audio extraction formats
	audioFormats := map[string]bool{
		"opus":   true,
		"mp3":    true,
		"m4a":    true,
		"aac":    true,
		"flac":   true,
		"wav":    true,
		"vorbis": true,
		"alac":   true,
	}

	if audioFormats[normFormat] {
		args = append(args,
			"-x",
			"--audio-format", normFormat,
			"--audio-quality", "0",
			"-f", "ba/ba*/b",
		)
	} else {
		switch normFormat {
		case "", "best":
			args = append(args, "-f", "bv*+ba/b/best")
		case "mp4":
			args = append(args, "-f", "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b/best", "--merge-output-format", "mp4")
		case "mkv":
			args = append(args, "-f", "bv*+ba/b/best", "--merge-output-format", "mkv")
		case "webm":
			args = append(args, "-f", "bv*[ext=webm]+ba[ext=webm]/bv*+ba/b/best", "--merge-output-format", "webm")
		default:
			args = append(args, "-f", fmt.Sprintf("%s/bv*+ba/b/best", format))
		}
	}

	args = append(args, targetURL)
	return args
}
