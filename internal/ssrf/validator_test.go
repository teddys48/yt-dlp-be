package ssrf

import (
	"testing"
)

func TestValidateURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"Valid YouTube URL", "https://www.youtube.com/watch?v=dQw4w9WgXcQ", false},
		{"Valid Vimeo URL", "https://vimeo.com/76979871", false},
		{"IPv4 Loopback", "http://127.0.0.1/admin", true},
		{"IPv4 Private Class A", "http://10.0.0.1/internal", true},
		{"IPv4 Private Class C", "http://192.168.1.1/", true},
		{"Cloud Metadata Service", "http://169.254.169.254/latest/meta-data/", true},
		{"Localhost Domain", "http://localhost:8080/api", true},
		{"Internal Domain", "http://server.internal/secret", true},
		{"File Scheme", "file:///etc/passwd", true},
		{"FTP Scheme", "ftp://example.com/file", true},
		{"Empty URL", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateURL(tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateURL() error = %v, wantErr %v for url %s", err, tt.wantErr, tt.url)
			}
		})
	}
}
