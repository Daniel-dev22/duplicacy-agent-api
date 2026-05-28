package main

import "testing"

// A fleetClient can be sent to unregister twice (sendOne on a full buffer +
// the read-pump defer on disconnect). The second unregister must be a no-op,
// not a double close(c.send) — which previously panicked the whole agent with
// "close of closed channel" under bursty job churn.
func TestFleetHubDoubleUnregisterNoPanic(t *testing.T) {
	h := newFleetHub(nil)
	c := &fleetClient{send: make(chan fleetEvent, 1)}

	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()

	h.handleUnregister(c) // first: removes + closes
	h.handleUnregister(c) // second: must not panic / double-close

	// send channel must be closed exactly once.
	if _, ok := <-c.send; ok {
		t.Fatal("expected c.send to be closed after unregister")
	}

	// client must be gone from the registry.
	h.mu.RLock()
	_, present := h.clients[c]
	h.mu.RUnlock()
	if present {
		t.Fatal("client still present after unregister")
	}
}

// Unregistering a client that was never registered must also be a safe no-op
// (it must not close a channel the hub never owned).
func TestFleetHubUnregisterUnknownClientNoClose(t *testing.T) {
	h := newFleetHub(nil)
	c := &fleetClient{send: make(chan fleetEvent, 1)}

	h.handleUnregister(c) // never registered

	// channel must remain open (a send then succeeds).
	select {
	case c.send <- fleetEvent{}:
	default:
		t.Fatal("expected open send channel for never-registered client")
	}
}
