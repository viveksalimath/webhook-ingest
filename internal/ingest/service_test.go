package ingest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestConcurrentDuplicateDeliveriesDoNotDoubleCount(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	const concurrency = 30
	var wg sync.WaitGroup
	wg.Add(concurrency)
	start := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			<-start
			resp := post(t, srv.URL+"/webhooks/calls", body)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("got %d, want 200", resp.StatusCode)
			}
		}()
	}
	close(start)
	wg.Wait()

	var eventCount int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("scan event count: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("events count = %d, want 1", eventCount)
	}

	stats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if stats.CallCount != 1 || stats.TotalDurationSec != 143 {
		t.Fatalf("durable stats: got CallCount=%d TotalDurationSec=%d, want 1 and 143",
			stats.CallCount, stats.TotalDurationSec)
	}

	// Verify in-memory stats cache returned by endpoint
	resp, err := http.Get(srv.URL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get stats: got %d, want 200", resp.StatusCode)
	}
	var cachedStats struct {
		CallCount        int64 `json:"call_count"`
		TotalDurationSec int64 `json:"total_duration_sec"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cachedStats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if cachedStats.CallCount != 1 || cachedStats.TotalDurationSec != 143 {
		t.Fatalf("cached stats: got CallCount=%d TotalDurationSec=%d, want 1 and 143",
			cachedStats.CallCount, cachedStats.TotalDurationSec)
	}
}

func TestRecordingIsMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// Wait enough time for recordingWork (50ms) to finish
	time.Sleep(150 * time.Millisecond)

	var processed bool
	row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan recording_processed: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true after background processing")
	}
}

