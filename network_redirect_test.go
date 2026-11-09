package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The controller client must not carry the node bearer to a redirect target
// on another host (kit transport behaviour, pinned here at the call site).
func TestControlCenterClientDoesNotLeakBearerOnRedirect(t *testing.T) {
	var got string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	c, err := buildControlCenterClient(Config{BearerToken: "s3cret"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Post(origin.URL, "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got != "" {
		t.Fatalf("bearer leaked to redirect target: %q", got)
	}
}
