package ssrf

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

var privateIPBlocks []*net.IPNet

func init() {
	cidrs := []string{
		"127.0.0.0/8",    // IPv4 Loopback
		"::1/128",        // IPv6 Loopback
		"10.0.0.0/8",     // RFC 1918 Private
		"172.16.0.0/12",  // RFC 1918 Private
		"192.168.0.0/16", // RFC 1918 Private
		"169.254.0.0/16", // Link-Local / Cloud Metadata (AWS/GCP/Azure)
		"fe80::/10",      // IPv6 Link-Local
		"fc00::/7",       // IPv6 Unique Local Address
		"100.64.0.0/10",  // Carrier-grade NAT
		"192.0.2.0/24",   // TEST-NET-1
		"198.51.100.0/24",// TEST-NET-2
		"203.0.113.0/24", // TEST-NET-3
		"224.0.0.0/4",    // Multicast
		"240.0.0.0/4",    // Reserved
		"0.0.0.0/8",      // Current network
	}

	for _, cidr := range cidrs {
		_, block, err := net.ParseCIDR(cidr)
		if err == nil {
			privateIPBlocks = append(privateIPBlocks, block)
		}
	}
}

// ValidateURL performs strict checks against SSRF vulnerabilities.
func ValidateURL(rawURL string) error {
	if strings.TrimSpace(rawURL) == "" {
		return fmt.Errorf("URL cannot be empty")
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL format: %w", err)
	}

	// 1. Only allow HTTP and HTTPS schemes
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("unsupported protocol scheme: %s (only http and https are allowed)", scheme)
	}

	hostname := parsed.Hostname()
	if hostname == "" {
		return fmt.Errorf("missing host in URL")
	}

	lowerHost := strings.ToLower(hostname)

	// 2. Reject known internal hostnames
	if lowerHost == "localhost" ||
		strings.HasSuffix(lowerHost, ".local") ||
		strings.HasSuffix(lowerHost, ".internal") ||
		strings.HasSuffix(lowerHost, ".localhost") ||
		lowerHost == "metadata.google.internal" {
		return fmt.Errorf("access to internal hostname '%s' is prohibited", hostname)
	}

	// 3. If hostname is directly an IP, parse and check
	if ip := net.ParseIP(hostname); ip != nil {
		if isPrivateIP(ip) {
			return fmt.Errorf("access to private IP address '%s' is prohibited", ip.String())
		}
		return nil
	}

	// 4. Resolve domain name to IP addresses and check all returned IPs
	ips, err := net.LookupIP(hostname)
	if err != nil {
		return fmt.Errorf("failed to resolve domain '%s': %w", hostname, err)
	}

	if len(ips) == 0 {
		return fmt.Errorf("no IP address found for host '%s'", hostname)
	}

	for _, ip := range ips {
		if isPrivateIP(ip) {
			return fmt.Errorf("domain '%s' resolves to private IP address '%s', access prohibited", hostname, ip.String())
		}
	}

	return nil
}

func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}

	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}
