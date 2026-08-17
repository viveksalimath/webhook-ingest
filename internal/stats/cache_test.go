package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	c := stats.NewCache()
	const goroutines = 20
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				c.Record("acc_concurrent", 10)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = c.Get("acc_concurrent")
			}
		}()
	}

	wg.Wait()

	got := c.Get("acc_concurrent")
	expectedCount := int64(goroutines * iterations)
	expectedDuration := expectedCount * 10
	if got.CallCount != expectedCount || got.TotalDurationSec != expectedDuration {
		t.Fatalf("got %+v, want CallCount=%d TotalDurationSec=%d", got, expectedCount, expectedDuration)
	}
}

