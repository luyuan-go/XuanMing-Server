package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/services/runtime/player_locator/internal/data"
)

type failingLocationRepo struct{ err error }

func (r failingLocationRepo) SetGuarded(context.Context, uint64, data.LocationRecord, time.Duration, int, func(data.LocationRecord, bool) error) error {
	return r.err
}

func (r failingLocationRepo) Get(context.Context, uint64) (data.LocationRecord, bool, error) {
	return data.LocationRecord{}, false, r.err
}

func (r failingLocationRepo) BatchGet(context.Context, []uint64) (map[uint64]data.LocationRecord, error) {
	return nil, r.err
}

func (r failingLocationRepo) RefreshHubLocations(context.Context, string, []uint64, time.Duration, time.Duration) (int, error) {
	return 0, r.err
}

func (r failingLocationRepo) ActivateHubPresence(context.Context, uint64, data.HubPresenceFence, time.Duration) (bool, error) {
	return false, r.err
}
func (r failingLocationRepo) ValidateHubPresence(context.Context, uint64, data.HubPresenceFence) (bool, error) {
	return false, r.err
}

func (r failingLocationRepo) ShrinkHubTTL(context.Context, string, uint64, data.HubPresenceFence, time.Duration) (bool, bool, error) {
	return false, false, r.err
}

func (r failingLocationRepo) RecordLastSeen(context.Context, uint64, data.HubPresenceFence, int64, time.Duration) (bool, int64, error) {
	return false, 0, r.err
}

func (r failingLocationRepo) BatchGetLastSeen(context.Context, []uint64) (map[uint64]int64, error) {
	return nil, r.err
}

func (r failingLocationRepo) Delete(context.Context, uint64) error { return r.err }

func TestLocationRead_DependencyFailureNeverMasqueradesAsOffline(t *testing.T) {
	dependencyErr := errors.New("redis unavailable")
	uc := NewLocatorUsecase(failingLocationRepo{err: dependencyErr}, 30*time.Second)

	t.Run("single get", func(t *testing.T) {
		got, err := uc.GetLocation(context.Background(), 42)
		if !errors.Is(err, dependencyErr) {
			t.Fatalf("GetLocation err=%v want dependency error", err)
		}
		if got.State == LocationStateOffline {
			t.Fatalf("dependency failure must not be returned as OFFLINE: %+v", got)
		}
	})

	t.Run("batch get", func(t *testing.T) {
		got, err := uc.BatchGetLocation(context.Background(), []uint64{42, 43})
		if !errors.Is(err, dependencyErr) {
			t.Fatalf("BatchGetLocation err=%v want dependency error", err)
		}
		if got != nil {
			t.Fatalf("dependency failure must not return an empty map that callers treat as offline: %+v", got)
		}
	})
}

func (r failingLocationRepo) TouchAlive(context.Context, uint64, int64, time.Duration) error {
	return r.err
}
