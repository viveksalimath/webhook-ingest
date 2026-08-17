// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one call-completion webhook delivery.
type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

// Stats is the durable per-account aggregate.
type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Store is a Postgres-backed repository.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool bounded to maxConns.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for tests and ad-hoc queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// EventExists reports whether an event with this ID has already been stored.
func (s *Store) EventExists(ctx context.Context, eventID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM events WHERE event_id = $1 LIMIT 1`, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// IngestEvent atomically records the event delivery, updates the call record,
// and increments account statistics within a single Postgres transaction.
// If the event_id has already been processed, it returns (false, nil) without modifying
// call records or account statistics.
func (s *Store) IngestEvent(ctx context.Context, e Event) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING id`,
		e.EventID, e.CallID, e.AccountID, e.Payload).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// Event was already ingested previously (duplicate delivery)
		return false, nil
	}
	if err != nil {
		return false, err
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	if err != nil {
		return false, err
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET
		     call_count         = account_stats.call_count + 1,
		     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		e.AccountID, e.DurationSec)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// InsertEvent stores the raw delivery.
func (s *Store) InsertEvent(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	return err
}

// UpsertCall creates or refreshes the call record for this event.
func (s *Store) UpsertCall(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	return err
}

// MarkRecordingProcessed flags the call's recording as handled.
func (s *Store) MarkRecordingProcessed(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE calls SET recording_processed = TRUE, updated_at = now()
		 WHERE call_id = $1`, callID)
	return err
}

// IncrementAccountStats folds one completed call into the durable aggregate.
func (s *Store) IncrementAccountStats(ctx context.Context, accountID string, durationSec int) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET
		     call_count         = account_stats.call_count + 1,
		     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		accountID, durationSec)
	return err
}

// AccountStats reads the durable aggregate. A missing account reads as zero.
func (s *Store) AccountStats(ctx context.Context, accountID string) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}

// CallRecord represents a stored call record.
type CallRecord struct {
	CallID             string    `json:"call_id"`
	AccountID          string    `json:"account_id"`
	Status             string    `json:"status"`
	DurationSec        int       `json:"duration_sec"`
	RecordingURL       string    `json:"recording_url"`
	RecordingProcessed bool      `json:"recording_processed"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// Summary holds global system totals for monitoring.
type Summary struct {
	TotalEvents      int64 `json:"total_events"`
	TotalCalls       int64 `json:"total_calls"`
	ProcessedCalls   int64 `json:"processed_calls"`
	TotalDurationSec int64 `json:"total_duration_sec"`
	UniqueAccounts   int64 `json:"unique_accounts"`
}

// RecentCalls returns the most recent calls.
func (s *Store) RecentCalls(ctx context.Context, limit int) ([]CallRecord, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx,
		`SELECT call_id, account_id, status, duration_sec, COALESCE(recording_url, ''), recording_processed, updated_at
		 FROM calls
		 ORDER BY updated_at DESC
		 LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]CallRecord, 0)
	for rows.Next() {
		var c CallRecord
		if err := rows.Scan(&c.CallID, &c.AccountID, &c.Status, &c.DurationSec, &c.RecordingURL, &c.RecordingProcessed, &c.UpdatedAt); err != nil {
			return nil, err
		}
		records = append(records, c)
	}
	return records, rows.Err()
}

// SystemSummary returns high-level counts for monitoring.
func (s *Store) SystemSummary(ctx context.Context) (Summary, error) {
	var sum Summary
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&sum.TotalEvents)
	_ = s.pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE recording_processed = true) FROM calls`).Scan(&sum.TotalCalls, &sum.ProcessedCalls)
	_ = s.pool.QueryRow(ctx, `SELECT count(*), COALESCE(sum(total_duration_sec), 0) FROM account_stats`).Scan(&sum.UniqueAccounts, &sum.TotalDurationSec)
	return sum, nil
}

