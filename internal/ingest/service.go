// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store     *store.Store
	cache     *stats.Cache
	rdb       *redis.Client
	log       *slog.Logger
	jobs      chan store.Event
	wg        sync.WaitGroup
	workerCtx context.Context
	cancel    context.CancelFunc
}

// New builds a Service and starts background workers.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	workerCtx, cancel := context.WithCancel(context.Background())
	svc := &Service{
		store:     s,
		cache:     c,
		rdb:       rdb,
		log:       log,
		jobs:      make(chan store.Event, 1000),
		workerCtx: workerCtx,
		cancel:    cancel,
	}

	const numWorkers = 5
	for i := 0; i < numWorkers; i++ {
		svc.wg.Add(1)
		go svc.worker()
	}

	return svc
}

// worker processes recording jobs until the jobs channel is closed.
func (s *Service) worker() {
	defer s.wg.Done()
	for rec := range s.jobs {
		if err := s.processRecording(s.workerCtx, rec); err != nil {
			s.log.Error("process recording failed", "call_id", rec.CallID, "err", err)
		}
	}
}

// Stats returns the totals for an account, checking the in-memory cache
// first and falling back to Postgres on a cold-cache miss.
func (s *Service) Stats(accountID string) stats.AccountStats {
	if st, ok := s.cache.Lookup(accountID); ok {
		return st
	}

	dbStats, err := s.store.AccountStats(context.Background(), accountID)
	if err != nil {
		s.log.Error("load account stats from store failed", "account_id", accountID, "err", err)
		return stats.AccountStats{}
	}

	st := stats.AccountStats{
		CallCount:        dbStats.CallCount,
		TotalDurationSec: dbStats.TotalDurationSec,
	}
	s.cache.Set(accountID, st)
	return st
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Enqueue recording processing asynchronously into worker queue
	if rec.RecordingURL != "" {
		select {
		case s.jobs <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done. It uses a decoupled background context to ensure execution
// is not aborted when the original HTTP request finishes.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	jobCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	select {
	case <-time.After(recordingWork):
	case <-jobCtx.Done():
		return jobCtx.Err()
	}

	return s.store.MarkRecordingProcessed(jobCtx, rec.CallID)
}

// Shutdown gracefully terminates the service by draining background workers.
func (s *Service) Shutdown(ctx context.Context) error {
	close(s.jobs)

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.cancel()
		return ctx.Err()
	}
}

