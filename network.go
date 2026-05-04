package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

// buildControlCenterClient returns the process-global pooled HTTP client used
// for all agent → controller traffic (event push, schedule pull, filter pull).
//
// Mirrors controller router's `crossSiteClient` pattern but with one twist:
// the agent runs on a Docker host (not in the K8s cluster), so it can't resolve
// `controller-api.<domain>` to the local Traefik via cluster DNS. Instead
// the DialContext resolves the Docker DNS name (default "traefik") to the local
// Traefik container's IP and dials that on TraefikDialPort, while preserving
// the original Host (and thus TLS SNI) for Traefik's IngressRoute matching.
//
// Result: traffic stays on the docker bridge network and never leaves the host,
// while Traefik still sees the public hostname and routes accordingly.
//
//   - mTLS client cert if configured (required by /api/duplicacy/jobs/*/event)
//   - MaxIdleConns 20 / per-host 10
//   - IdleConnTimeout 0 (never expire — rely on TCP keepalive)
//   - 15s TCP keepalive (more frequent since this hits Traefik through Docker bridge)
//
// Per project CLAUDE.md "Standard HTTP Client Patterns": process-global, no
// per-request client instantiation anywhere in the codebase.
func buildControlCenterClient(cfg Config) (*http.Client, error) {
	tlsCfg := &tls.Config{}

	if cfg.ControlCenterCAFile != "" {
		caBytes, err := os.ReadFile(cfg.ControlCenterCAFile)
		if err != nil {
			return nil, fmt.Errorf("read controller CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("failed to parse CA bundle %s", cfg.ControlCenterCAFile)
		}
		tlsCfg.RootCAs = pool
	}

	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load mTLS client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 15 * time.Second,
	}

	traefikTarget := net.JoinHostPort(cfg.TraefikDockerDNS, cfg.TraefikDialPort)

	transport := &http.Transport{
		TLSClientConfig:     tlsCfg,
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     0,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			// Always dial the local Traefik container (resolved fresh from Docker DNS
			// each connect — picks up Traefik restarts without an agent restart).
			// The original `addr` is intentionally ignored; TLS SNI comes from
			// TLSClientConfig.ServerName which the http package auto-derives from
			// the request URL host, preserving Traefik's IngressRoute matching.
			return dialer.DialContext(ctx, network, traefikTarget)
		},
	}

	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}, nil
}
