package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
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

// TestRecordingIsMarkedProcessedAfterIngest verifies that the recording
// goroutine runs to completion even though the HTTP handler's context is
// cancelled the moment the response is written.
// (Regression for Bug 2: goroutine captured the request ctx, which was
// immediately cancelled, silently aborting the MarkRecordingProcessed query.)
func TestRecordingIsMarkedProcessedAfterIngest(t *testing.T) {
	srv, st, svc := testutil.NewServerFull(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// Shutdown blocks until the background recording goroutine finishes.
	svc.Shutdown()

	var processed bool
	if err := st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID,
	).Scan(&processed); err != nil {
		t.Fatalf("scan recording_processed: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed=true after Shutdown(); goroutine likely ran with a cancelled context")
	}
}

// TestConcurrentDuplicateDeliveryIsIdempotent fires several concurrent
// requests with the same event_id and asserts that exactly one event row
// is persisted and account stats are not double-counted.
// (Regression for Bug 3: EventExists+InsertEvent had a TOCTOU race and the
// events table lacked a UNIQUE constraint, so concurrent races could insert
// multiple rows and over-count statistics.)
func TestConcurrentDuplicateDeliveryIsIdempotent(t *testing.T) {
	srv, st, svc := testutil.NewServerFull(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			post(t, srv.URL+"/webhooks/calls", body)
		}()
	}
	wg.Wait()
	svc.Shutdown()

	var eventRows int
	if err := st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`, eventID,
	).Scan(&eventRows); err != nil {
		t.Fatalf("scan events count: %v", err)
	}
	if eventRows != 1 {
		t.Fatalf("events table has %d rows for event_id %q, want exactly 1", eventRows, eventID)
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 {
		t.Fatalf("call_count=%d, want 1 (stats were double-counted)", got.CallCount)
	}
	if got.TotalDurationSec != 143 {
		t.Fatalf("total_duration_sec=%d, want 143", got.TotalDurationSec)
	}
}

// TestInFlightRecordingSurvivesShutdown verifies that Shutdown() blocks until
// the background recording goroutine completes, so recording_processed is set
// even when the process receives SIGTERM while a recording job is in flight.
// (Regression for Bug 4: goroutines were fire-and-forget with no WaitGroup,
// so they were silently killed on every deploy.)
func TestInFlightRecordingSurvivesShutdown(t *testing.T) {
	_, st, svc := testutil.NewServerFull(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  10,
		RecordingURL: "https://example.com/r.wav",
		OccurredAt:   time.Now(),
	}
	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Shutdown must block until the goroutine marks the recording processed.
	svc.Shutdown()

	var processed bool
	if err := st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID,
	).Scan(&processed); err != nil {
		t.Fatalf("scan recording_processed: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed=true after Shutdown(); goroutine was killed before finishing")
	}
}
