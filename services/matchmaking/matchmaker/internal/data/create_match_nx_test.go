// create_match_nx_test.go —— CreateMatch SETNX 回归(anti-abuse §6 第 7 项):
// 权威 match 记录绝不允许被同 ID 后来者覆盖(旧「无 NX SET」是 requeue 风暴的放大器)。
package data

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/errcode"

	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
)

func TestCreateMatchRejectsDuplicateID(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewRedisMatchRepo(rdb, "")
	ctx := context.Background()

	first := &matchv1.MatchStorageRecord{
		MatchId:           4242,
		Stage:             matchv1.MatchStage_MATCH_STAGE_CONFIRM,
		TicketIds:         []uint64{1},
		ConfirmDeadlineMs: time.Now().Add(time.Minute).UnixMilli(),
	}
	if err := repo.CreateMatch(ctx, first, 30*time.Minute); err != nil {
		t.Fatalf("first create: %v", err)
	}
	overwrite := &matchv1.MatchStorageRecord{
		MatchId:           4242,
		Stage:             matchv1.MatchStage_MATCH_STAGE_QUEUEING,
		TicketIds:         []uint64{2},
		ConfirmDeadlineMs: time.Now().Add(time.Minute).UnixMilli(),
	}
	err := repo.CreateMatch(ctx, overwrite, 30*time.Minute)
	if errcode.As(err) != errcode.ErrAlreadyExists {
		t.Fatalf("duplicate create want ErrAlreadyExists, got %v", err)
	}
	// 原记录必须原封未动。
	got, found, gerr := repo.GetMatch(ctx, 4242)
	if gerr != nil || !found || got.GetStage() != matchv1.MatchStage_MATCH_STAGE_CONFIRM || got.GetTicketIds()[0] != 1 {
		t.Fatalf("original match mutated: %+v found=%v err=%v", got, found, gerr)
	}
}
