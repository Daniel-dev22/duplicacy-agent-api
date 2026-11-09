package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// The controller client must refuse a redirect to another origin: neither the
// node bearer (kit transport) nor the replayed 307 body may reach the target.
func TestControlCenterClientRefusesCrossOriginRedirect(t *testing.T) {
	var targetHits int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&targetHits, 1)
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
	resp, err := c.Post(origin.URL, "text/plain", strings.NewReader("SECRET-BODY"))
	if err == nil {
		resp.Body.Close()
		t.Fatal("cross-origin redirect was followed")
	}
	if n := atomic.LoadInt32(&targetHits); n != 0 {
		t.Fatalf("redirect target received %d request(s)", n)
	}
}
