package main

// Process-global pooled HTTP client used for every agent → controller
// call (event push, schedule pull, filter pull, credential vend).
//
// Authentication model:
//   - Transport-layer mTLS is handled at the Traefik boundary on each side
//     (the host's docker Traefik for NAS hosts, k3s Traefik for k3s nodes).
//     The agent does NOT load or present any client certificate of its own.
//   - Application-layer per-node identity is conveyed via the BearerToken
//     header attached automatically by the round-tripper below.
//
// Per project CLAUDE.md "Standard HTTP Client Patterns":
//   - process-global, no per-request client instantiation
//   - MaxIdleConns 20 / per-host 10
//   - IdleConnTimeout 0 (rely on TCP keepalive)
//   - 15s TCP keepalive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// readErrorBody reads up to 4 KiB of an error response body as a UTF-8 string,
// best-effort. Used to surface controller error messages in fetchSecrets.
func readErrorBody(r io.Reader) string {
	const max = 4 * 1024
	buf := make([]byte, max)
	n, _ := io.ReadFull(io.LimitReader(r, max), buf)
	if n == 0 {
		return ""
	}
	return string(buf[:n])
}

// decodeJSONBody decodes a JSON response body into v.
func decodeJSONBody(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(v)
}

// bearerAuthRoundTripper attaches the per-host bearer token to every outgoing
// agent → controller request. Wraps the underlying transport unchanged so
// pooling/dialing semantics are preserved. Skips requests whose Authorization
// header is already set (so callers can override on a per-request basis).
type bearerAuthRoundTripper struct {
	rt    http.RoundTripper
	token string
}

func (b *bearerAuthRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if b.token != "" && r.Header.Get("Authorization") == "" {
		// Clone before mutating per net/http RoundTripper contract.
		r2 := r.Clone(r.Context())
		r2.Header.Set("Authorization", "Bearer "+b.token)
		return b.rt.RoundTrip(r2)
	}
	return b.rt.RoundTrip(r)
}

// buildControlCenterClient returns the pooled HTTP client. Two operating modes:
//
//   - **Direct mode** (TraefikDockerDNS == ""): the URL host is dialed
//     normally. Used on k3s nodes where the agent reaches cluster services
//     via flannel CNI.
//
//   - **Traefik-rewrite mode** (TraefikDockerDNS != ""): DialContext rewrites
//     the dial target to <TraefikDockerDNS>:<TraefikDialPort> while keeping
//     the original URL host as TLS SNI. Used on NAS hosts where the local
//     docker Traefik attaches the cluster's mTLS ServersTransport identity
//     and forwards into the cluster.
func buildControlCenterClient(cfg Config) (*http.Client, error) {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 15 * time.Second,
	}

	transport := &http.Transport{
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
		Transport: &bearerAuthRoundTripper{rt: transport, token: cfg.BearerToken},
	}, nil
}

// fetchSecrets calls controller's vending endpoint for one credential.
// The bearer-auth round-tripper attaches the Authorization header
// automatically; controller hashes the token, looks up the row in
// duplicacy_node_tokens, and asserts the stored node matches :node.
//
// Transient failures (network error, 5xx) are retried with exponential
// backoff inside the caller's ctx budget. Permanent failures (401, 403, 404)
// short-circuit immediately — there is no value in retrying an auth or
// provisioning mismatch.
//
// On HTTP error, the agent must NOT fall back to on-disk creds — the caller
// should fail the duplicacy invocation outright.
func fetchSecrets(ctx context.Context, client *http.Client, controlCenterURL, node, credentialID string) (SecretsBundle, error) {
	if client == nil {
		return SecretsBundle{}, fmt.Errorf("controller client not initialised")
	}
	if controlCenterURL == "" {
		return SecretsBundle{}, fmt.Errorf("controller URL not configured")
	}
	url := fmt.Sprintf("%s/api/duplicacy/credentials/%s/secrets-for-node/%s",
		controlCenterURL, credentialID, node)

	// Bounded retry on transient failures. 4 attempts × backoff (0, 500ms, 1s,
	// 2s) = ~3.5s worst case, fits inside prepareEnvForRepo's 30s ctx with
	// plenty of room for the actual request. Witness for this retry: the
	// 2026-05-27 02:07 controller DB pressure window — the agent saw transient
	// 503 on bearer-token lookup and dropped the schedule entirely; with retry,
	// the recovery 5s later would have picked it up.
	const maxAttempts = 4
	backoff := 500 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return SecretsBundle{}, fmt.Errorf("vend ctx done before retry: %w", ctx.Err())
			case <-time.After(backoff):
			}
			backoff *= 2
		}

		b, transient, err := fetchSecretsOnce(ctx, client, url, credentialID, node)
		if err == nil {
			return b, nil
		}
		if !transient {
			return SecretsBundle{}, err
		}
		lastErr = err
	}
	return SecretsBundle{}, fmt.Errorf("vend transient failure after %d attempts: %w", maxAttempts, lastErr)
}

// fetchSecretsOnce is a single attempt at the vend HTTP call. Returns
// (bundle, transient, error). When transient=true the caller may retry.
func fetchSecretsOnce(ctx context.Context, client *http.Client, url, credentialID, node string) (SecretsBundle, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return SecretsBundle{}, false, fmt.Errorf("build vend request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		// Network-layer errors (dial / TLS / read) are transient.
		return SecretsBundle{}, true, fmt.Errorf("call controller: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// fall through
	case http.StatusUnauthorized:
		return SecretsBundle{}, false, fmt.Errorf("controller rejected bearer token (provisioning issue?): %s", readErrorBody(resp.Body))
	case http.StatusForbidden:
		return SecretsBundle{}, false, fmt.Errorf("controller denied secrets for credential %s on node %s (no repo↔credential link or token/node mismatch)",
			credentialID, node)
	case http.StatusNotFound:
		return SecretsBundle{}, false, fmt.Errorf("credential %s not found on controller", credentialID)
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout, http.StatusInternalServerError:
		// Controller side hiccup (DB pressure, traefik upstream timeout,
		// rolling restart). Retry.
		return SecretsBundle{}, true, fmt.Errorf("controller transient %d: %s", resp.StatusCode, readErrorBody(resp.Body))
	default:
		return SecretsBundle{}, false, fmt.Errorf("controller returned %d: %s", resp.StatusCode, readErrorBody(resp.Body))
	}

	var b SecretsBundle
	if err := decodeJSONBody(resp.Body, &b); err != nil {
		return SecretsBundle{}, false, fmt.Errorf("decode vend response: %w", err)
	}
	if b.EncryptionPassword == "" {
		return SecretsBundle{}, false, fmt.Errorf("vend response missing encryption_password")
	}
	if b.StorageType == "" {
		return SecretsBundle{}, false, fmt.Errorf("vend response missing storage_type")
	}
	return b, false, nil
}
