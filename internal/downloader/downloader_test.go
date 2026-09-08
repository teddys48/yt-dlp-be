package downloader

import (
	"testing"
)

func TestBuildYtDlpArgs(t *testing.T) {
	tests := []struct {
		name         string
		format       string
		wantHasX     bool
		wantAudioFmt string
		wantMergeFmt string
	}{
		{
			name:         "Opus Audio Extraction",
			format:       "opus",
			wantHasX:     true,
			wantAudioFmt: "opus",
		},
		{
			name:         "MP3 Audio Extraction",
			format:       "mp3",
			wantHasX:     true,
			wantAudioFmt: "mp3",
		},
		{
			name:         "M4A Audio Extraction",
			format:       "m4a",
			wantHasX:     true,
			wantAudioFmt: "m4a",
		},
		{
			name:         "MP4 Video Remux",
			format:       "mp4",
			wantHasX:     false,
			wantMergeFmt: "mp4",
		},
		{
			name:         "MKV Video Remux",
			format:       "mkv",
			wantHasX:     false,
			wantMergeFmt: "mkv",
		},
		{
			name:     "Best Video Default",
			format:   "best",
			wantHasX: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := buildYtDlpArgs("./downloads/test-job-id_title.ext", "https://www.youtube.com/watch?v=dQw4w9WgXcQ", tt.format)

			hasX := false
			hasAddMetadata := false
			hasEmbedThumbnail := false
			foundAudioFmt := ""
			foundMergeFmt := ""

			for i, arg := range args {
				if arg == "-x" {
					hasX = true
				}
				if arg == "--add-metadata" {
					hasAddMetadata = true
				}
				if arg == "--embed-thumbnail" {
					hasEmbedThumbnail = true
				}
				if arg == "--audio-format" && i+1 < len(args) {
					foundAudioFmt = args[i+1]
				}
				if arg == "--merge-output-format" && i+1 < len(args) {
					foundMergeFmt = args[i+1]
				}
			}

			if !hasAddMetadata {
				t.Errorf("buildYtDlpArgs() missing --add-metadata flag")
			}
			if !hasEmbedThumbnail {
				t.Errorf("buildYtDlpArgs() missing --embed-thumbnail flag")
			}
			if hasX != tt.wantHasX {
				t.Errorf("buildYtDlpArgs() hasX = %v, want %v", hasX, tt.wantHasX)
			}
			if tt.wantAudioFmt != "" && foundAudioFmt != tt.wantAudioFmt {
				t.Errorf("buildYtDlpArgs() audio-format = %s, want %s", foundAudioFmt, tt.wantAudioFmt)
			}
			if tt.wantMergeFmt != "" && foundMergeFmt != tt.wantMergeFmt {
				t.Errorf("buildYtDlpArgs() merge-output-format = %s, want %s", foundMergeFmt, tt.wantMergeFmt)
			}
		})
	}
}
