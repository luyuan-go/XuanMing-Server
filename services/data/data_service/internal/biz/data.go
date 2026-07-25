// Package biz 是 data_service 的业务逻辑层(cache-aside 读写编排,2026-06-16)。
//
// 职责(docs/design/go-services.md §2.3):
//   - ReadPlayer:Redis 命中直返;miss 读 MySQL → 回填缓存 → 返回
//   - WritePlayer:MySQL 乐观锁版本写(UPDATE ... WHERE version=?)→ 删缓存
//   - InvalidateCache:主动删缓存
//
// 一致性约定:
//   - MySQL 是事实源(source of truth),Redis 仅旁路缓存,弱一致;
//   - 写采用 cache-aside「先写库、后删缓存」,删失败只告警不回滚(缓存最终随 TTL 失效);
//   - 不接 kafka:避免与 player.update 事件语义重复,缓存失效靠写后删 + 主动 InvalidateCache。
package biz

import (
	"context"

	klog "github.com/go-kratos/kratos/v2/log"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	datav1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/data_service/v1"

	"github.com/luyuancpp/pandora/services/data/data_service/internal/conf"
	"github.com/luyuancpp/pandora/services/data/data_service/internal/data"
)

// DataUsecase 是 data_service 业务逻辑核心。
type DataUsecase struct {
	store data.PlayerStore
	cache data.PlayerCache // 弱依赖,可为 nil(无缓存时直连 MySQL)
	cfg   conf.DataConf
	log   *klog.Helper

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分片,玩家数据 owner 落点观测退化为不打日志(行为不变)。
	// 分片部署时由 main 经 SetCellRouter 注入,写(WritePlayer)后额外打一条玩家数据 owner 落点
	// 观测(供分片上线核对玩家数据落点 == 玩家 owner cell,§4.2 line 142)。nil-safe。
	router *cellroute.Router
}

// NewDataUsecase 构造。cache 允许为 nil(缓存未配置时退化为直连 MySQL)。
func NewDataUsecase(store data.PlayerStore, cache data.PlayerCache, cfg conf.DataConf, logger klog.Logger) *DataUsecase {
	return &DataUsecase{
		store: store,
		cache: cache,
		cfg:   cfg,
		log:   plog.NewHelper(logger),
	}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),不做玩家数据 owner 落点观测,行为与历史
// 一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker / auction /
// battle_result / friend / chat / trade / dialogue / inventory / locator / push / team / player 一致)。
// Router 内部读路径无锁,并发安全。
func (u *DataUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// ReadPlayer cache-aside 读:缓存命中直返;miss 读 MySQL 并回填缓存。
//   - 玩家无数据 → (nil, false, nil),由 service 转 ErrNotFound。
func (u *DataUsecase) ReadPlayer(ctx context.Context, playerID uint64) (*datav1.PlayerData, bool, error) {
	if playerID == 0 {
		return nil, false, nil
	}

	// 1) 查缓存。读失败只告警,继续回落 MySQL。
	if u.cache != nil {
		if pd, hit, err := u.cache.Get(ctx, playerID); err != nil {
			u.log.WithContext(ctx).Warnw("msg", "cache_get_failed", "player_id", playerID, "err", err)
		} else if hit {
			return pd, true, nil
		}
	}

	// 2) 读 MySQL。
	pd, found, err := u.store.Read(ctx, playerID)
	if err != nil {
		// MySQL 是事实源:读失败经 service 层 in-band Code + nil error 返回,access log 记 rpc_ok,
		// 若不在此显式 ERROR,源库读故障线上零可见(§16 禁止静默吞错)。
		plog.With(ctx).Errorw("msg", "player_read_failed", "player_id", playerID, "err", err)
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	// 3) 回填缓存(失败只告警)。
	u.fillCache(ctx, pd)
	return pd, true, nil
}

// WritePlayer 乐观锁写 MySQL,成功后删缓存(cache-aside 先写库后删缓存)。
// 返回写入后的新版本号。版本不匹配 → ErrDataVersionMismatch。
//
// updateFields 是本次 UPDATE 要写的业务列(snake_case proto 字段名,来自 WritePlayerRequest.update_mask):
//   - 更新(pd.version>0)时**必须非空**:每个写方只声明自己认得的列,禁止空掩码全量覆盖——
//     否则旧副本一次全量写会把它不认得的新列清零,破坏零停机滚动升级(CLAUDE.md §9 不变量 17);
//   - 非空 → 只写掩码内的列(其余列保持库中原值)。
//
// 新建(version==0)始终整条 INSERT,updateFields 被忽略。
// 更新时掩码为空 → 返回 ErrInvalidArg;掩码含 player_id / version / 未知字段 → 返回 ErrInvalidArg。
func (u *DataUsecase) WritePlayer(ctx context.Context, pd *datav1.PlayerData, updateFields []string) (uint32, error) {
	if pd.GetPlayerId() == 0 {
		return 0, errInvalidPlayer()
	}
	// 校验 update_mask(仅对更新有意义;新建整条 INSERT 时掩码无效)。
	if pd.GetVersion() > 0 {
		if len(updateFields) == 0 {
			return 0, errcode.New(errcode.ErrInvalidArg,
				"update_mask required for player_data %d update (empty mask would overwrite unknown new columns)", pd.GetPlayerId())
		}
		for _, f := range updateFields {
			if !data.IsPlayerDataUpdatableField(f) {
				return 0, errcode.New(errcode.ErrInvalidArg,
					"invalid update_mask path %q (not an updatable player_data field)", f)
			}
		}
	}

	newVersion, err := u.store.Write(ctx, pd, updateFields)
	if err != nil {
		// 版本不匹配是良性乐观锁竞争(DEBUG);其余是源库写故障,必须 ERROR(否则经 in-band
		// Code 返回被 access log 记成 rpc_ok,与良性冲突无法区分,§9 单 owner 下频繁冲突还可能
		// 暴露并发写者)。
		if errcode.As(err) == errcode.ErrDataVersionMismatch {
			plog.With(ctx).Debugw("msg", "player_write_version_mismatch",
				"player_id", pd.GetPlayerId(), "expect_version", pd.GetVersion())
		} else {
			plog.With(ctx).Errorw("msg", "player_write_failed",
				"player_id", pd.GetPlayerId(), "version", pd.GetVersion(), "err", err)
		}
		return 0, err
	}

	// 写后删缓存(避免读到旧版本)。删失败只告警,缓存随 TTL 自然失效。
	if u.cache != nil {
		if err := u.cache.Del(ctx, pd.GetPlayerId()); err != nil {
			u.log.WithContext(ctx).Warnw("msg", "cache_del_after_write_failed", "player_id", pd.GetPlayerId(), "err", err)
		}
	}
	// 分片:PlayerData 行是 owner 数据,锁定玩家 owner cell(PlayerDataShardKey=player_id,
	// §4.2 line 142)。router 为 nil(单 Cell)→ 不打,行为与历史一致。
	u.logPlayerDataPlacement(ctx, pd.GetPlayerId(), "write_player")
	return newVersion, nil
}

// InvalidateCache 主动删缓存(供上层在外部直写 DB 后强制失效)。
func (u *DataUsecase) InvalidateCache(ctx context.Context, playerID uint64) error {
	if playerID == 0 {
		return errInvalidPlayer()
	}
	if u.cache == nil {
		return nil
	}
	if err := u.cache.Del(ctx, playerID); err != nil {
		u.log.WithContext(ctx).Warnw("msg", "cache_invalidate_failed", "player_id", playerID, "err", err)
		return err
	}
	return nil
}

// fillCache 回填缓存,失败只告警(缓存是旁路,不影响读正确性)。
func (u *DataUsecase) fillCache(ctx context.Context, pd *datav1.PlayerData) {
	if u.cache == nil {
		return
	}
	if err := u.cache.Set(ctx, pd, u.cfg.CacheTTL.Std()); err != nil {
		u.log.WithContext(ctx).Warnw("msg", "cache_set_failed", "player_id", pd.GetPlayerId(), "err", err)
	}
}

// errInvalidPlayer 返回 player_id 缺失的参数错误。
func errInvalidPlayer() error {
	return errcode.New(errcode.ErrInvalidArg, "player_id required")
}
