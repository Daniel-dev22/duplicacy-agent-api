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
// Two operating modes, picked by whether TraefikDockerDNS is set:
//
//   - **Direct mode** (TraefikDockerDNS == ""): the URL host is dialed normally.
//     Used on k3s nodes where the agent reaches the cluster via cluster DNS
//     (e.g. http://controller-router.controller.svc.cluster.local:5000)
//     directly through the flannel CNI — no Traefik in the path.
//
//   - **Traefik-rewrite mode** (TraefikDockerDNS != ""): the DialContext rewrites
//     the dial target to `<TraefikDockerDNS>:<TraefikDialPort>` while keeping
//     the original URL host as TLS SNI. Used on NAS / non-k3s docker hosts
//     where the local docker Traefik mirrors the K8s ServersTransport pattern
//     (attaches the mTLS client cert and forwards into the cluster).
//
// Per project CLAUDE.md "Standard HTTP Client Patterns": process-global, no
// per-request client instantiation anywhere in the codebase.
//
//   - MaxIdleConns 20 / per-host 10
//   - IdleConnTimeout 0 (never expire — rely on TCP keepalive)
//   - 15s TCP keepalive
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

	transport := &http.Transport{
		TLSClientConfig:     tlsCfg,
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     0,
	}

	if cfg.TraefikDockerDNS != "" {
		traefikTarget := net.JoinHostPort(cfg.TraefikDockerDNS, cfg.TraefikDialPort)
		transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			// Rewrite-mode: ignore URL host, dial Traefik fresh each connect so
			// a Traefik restart is picked up without an agent restart. TLS SNI
			// still comes from the URL host via http package auto-derivation.
			return dialer.DialContext(ctx, network, traefikTarget)
		}
	} else {
		transport.DialContext = dialer.DialContext
	}

	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}, nil
}
