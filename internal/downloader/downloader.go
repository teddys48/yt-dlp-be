package downloader

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
	cmd := getYtDlpCmd(ctx,
		"--dump-json",
		"--no-playlist",
		"--no-warnings",
		"--no-update",
		"--js-runtimes", "node",
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

	rawExt, _ := raw["ext"].(string)
	meta.FileName, meta.Extension = ResolveFileNameAndExt(meta.Title, "", rawExt)

	return meta, nil
}

// Download executes yt-dlp download with real-time stdout progress parsing and metadata embedding
func (d *Downloader) Download(
	ctx context.Context,
	jobID string,
	targetURL string,
	format string,
	onProgress ProgressCallback,
) (filePath string, fileName string, fileSize int64, title string, thumbnail string, duration float64, err error) {

	// 1. Fetch metadata first to get title, thumbnail, duration
	meta, metaErr := d.ExtractMetadata(ctx, targetURL)
	var metaExt string
	if metaErr == nil && meta != nil {
		title = meta.Title
		thumbnail = meta.Thumbnail
		duration = meta.Duration
		metaExt = meta.Extension
	}

	outputTemplate := filepath.Join(d.DownloadDir, fmt.Sprintf("%s_%%(title).50s.%%(ext)s", jobID))

	args := buildYtDlpArgs(outputTemplate, targetURL, format)

	cmd := getYtDlpCmd(ctx, args...)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", "", 0, title, thumbnail, duration, fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return "", "", 0, title, thumbnail, duration, fmt.Errorf("failed to start yt-dlp process: %w", err)
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
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return "", "", 0, title, thumbnail, duration, fmt.Errorf("job download aborted: %w", ctx.Err())
	}

	if cmdErr != nil {
		return "", "", 0, title, thumbnail, duration, fmt.Errorf("yt-dlp failed: %w, stderr: %s", cmdErr, stderrBuf.String())
	}

	// Find downloaded file matching pattern jobID_*
	matches, globErr := filepath.Glob(filepath.Join(d.DownloadDir, jobID+"_*"))
	if globErr != nil || len(matches) == 0 {
		return "", "", 0, title, thumbnail, duration, fmt.Errorf("downloaded file not found on disk after completion")
	}

	finalPath := matches[0]
	actualExt := strings.TrimPrefix(filepath.Ext(finalPath), ".")
	if actualExt != "" {
		metaExt = actualExt
	}

	fileName, _ = ResolveFileNameAndExt(title, format, metaExt)

	fInfo, err := os.Stat(finalPath)
	if err != nil {
		return finalPath, fileName, 0, title, thumbnail, duration, nil
	}

	return finalPath, fileName, fInfo.Size(), title, thumbnail, duration, nil
}

// GetYtDlpVersion retrieves the currently installed yt-dlp version string
func (d *Downloader) GetYtDlpVersion(ctx context.Context) (string, error) {
	cmd := getYtDlpCmd(ctx, "--version")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to check yt-dlp version: %w, stderr: %s", err, stderr.String())
	}

	return strings.TrimSpace(stdout.String()), nil
}

// UpdateYtDlp executes self-update for yt-dlp
func (d *Downloader) UpdateYtDlp(ctx context.Context) (oldVersion string, newVersion string, err error) {
	oldVer, _ := d.GetYtDlpVersion(ctx)

	cmd := getYtDlpCmd(ctx, "-U")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	if runErr != nil {
		slog.Warn("yt-dlp -U failed, attempting fallback update script", slog.String("stderr", stderr.String()))
		targetBin := findYtDlpBinaryPath()
		fallbackCmd := exec.CommandContext(ctx, "sh", "-c", fmt.Sprintf("curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o %s && chmod a+rx %s", targetBin, targetBin))
		if fbErr := fallbackCmd.Run(); fbErr != nil {
			return oldVer, oldVer, fmt.Errorf("yt-dlp update failed: %w, stderr: %s", runErr, stderr.String())
		}
	}

	newVer, _ := d.GetYtDlpVersion(ctx)
	return oldVer, newVer, nil
}

func getYtDlpCmd(ctx context.Context, args ...string) *exec.Cmd {
	bin := findYtDlpBinaryPath()
	return exec.CommandContext(ctx, bin, args...)
}

func findYtDlpBinaryPath() string {
	if path, err := exec.LookPath("yt-dlp"); err == nil {
		return path
	}
	candidates := []string{
		"/usr/local/bin/yt-dlp",
		"/usr/bin/yt-dlp",
		"/home/teddy/.local/bin/yt-dlp",
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "yt-dlp"
}

// ResolveFileNameAndExt generates clean filename matching original source title and format
func ResolveFileNameAndExt(title string, format string, metaExt string) (fileName string, extension string) {
	normFormat := strings.ToLower(strings.TrimSpace(format))

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
	videoContainers := map[string]bool{
		"mp4":  true,
		"mkv":  true,
		"webm": true,
		"avi":  true,
		"mov":  true,
	}

	if audioFormats[normFormat] {
		extension = normFormat
	} else if videoContainers[normFormat] {
		extension = normFormat
	} else if metaExt != "" {
		extension = metaExt
	} else {
		extension = "mp4"
	}

	cleanTitle := sanitizeFilename(title)
	if cleanTitle == "" {
		cleanTitle = "download"
	}

	fileName = fmt.Sprintf("%s.%s", cleanTitle, extension)
	return fileName, extension
}

func sanitizeFilename(name string) string {
	reg := regexp.MustCompile(`[\\/:*?"<>|]`)
	cleaned := reg.ReplaceAllString(name, "_")
	cleaned = strings.TrimSpace(cleaned)
	return cleaned
}

func buildYtDlpArgs(outputTemplate, targetURL, format string) []string {
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
