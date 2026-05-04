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
// Pattern: project CLAUDE.md "crossSiteClient" — long-lived TLS hop through Traefik.
//   - mTLS client cert if configured (required by /api/duplicacy/jobs/*/event)
//   - MaxIdleConns 20 / per-host 10
//   - IdleConnTimeout 0 (never expire — rely on TCP keepalive)
//   - 15s TCP keepalive (more frequent than internal-only since this hits Traefik)
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

	transport := &http.Transport{
		TLSClientConfig:     tlsCfg,
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     0,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 15 * time.Second,
			}).DialContext(ctx, network, addr)
		},
	}

	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}, nil
}
