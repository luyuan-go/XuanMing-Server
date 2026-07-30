package biz

import (
	"context"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"testing"

	chatv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/chat/v1"
	"github.com/luyuancpp/pandora/services/social/chat/internal/conf"
)

type paginationSpyRepo struct {
	limit    int
	beforeMs int64
	calls    int
}

func (*paginationSpyRepo) SavePrivate(context.Context, *chatv1.ChatMessage) error { return nil }

func (r *paginationSpyRepo) ListPrivate(_ context.Context, _, _ uint64, limit int, beforeMs int64) ([]*chatv1.ChatMessage, error) {
	r.calls++
	r.limit = limit
	r.beforeMs = beforeMs
	return nil, nil
}

func (*paginationSpyRepo) SweepMessagesBefore(_ context.Context, mode dbguard.Mode, _ uint64, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

func TestPullHistory_ClampsLimitAndPreservesCursor(t *testing.T) {
	tests := []struct {
		name      string
		requested int
		want      int
	}{
		{name: "zero uses service maximum", requested: 0, want: 50},
		{name: "negative uses service maximum", requested: -1, want: 50},
		{name: "oversized clamps", requested: 5000, want: 50},
		{name: "valid limit preserved", requested: 17, want: 17},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &paginationSpyRepo{}
			uc := NewChatUsecase(repo, nil, nil, nil, nil, conf.ChatConf{
				MaxContentLen: 256,
				HistoryLimit:  50,
			})
			const cursor = int64(1_700_000_000_123)

			if _, err := uc.PullHistory(context.Background(), 1,
				chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, tt.requested, cursor); err != nil {
				t.Fatalf("PullHistory err: %v", err)
			}
			if repo.calls != 1 {
				t.Fatalf("ListPrivate calls=%d want=1", repo.calls)
			}
			if repo.limit != tt.want {
				t.Fatalf("repo limit=%d want=%d", repo.limit, tt.want)
			}
			if repo.beforeMs != cursor {
				t.Fatalf("repo before_ms=%d want=%d", repo.beforeMs, cursor)
			}
		})
	}
}
