package service

import (
	"context"
	"strings"

	"github.com/luyuancpp/pandora/pkg/configtable"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	configv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// ConfigTableAdminService 实现 configv1.ConfigTableAdminServiceServer:
// 配置表热更受控入口(hotreload doc §6)。仅 config_table.dir 配置时注册。
//
// 信任模型与 ReleaseMatch 相同:内部接口,不经 Envoy 暴露给客户端;
// 再加 callerID 兜底门——带玩家身份(经 Envoy JWT 注入)的调用一律拒绝,
// 玩家永远无法触发 reload。
type ConfigTableAdminService struct {
	store     *configtable.Store
	activeDir string
}

// NewConfigTableAdminService 构造热更入口。store 为 main 启动时已加载成功的容器。
func NewConfigTableAdminService(store *configtable.Store, activeDir string) *ConfigTableAdminService {
	return &ConfigTableAdminService{store: store, activeDir: activeDir}
}

// ReloadConfigTable 重读 active 目录并原子切换。
// 幂等:同 version 重复调用 no-op;失败保留旧表并返回定位原因(hotreload doc §6 语义)。
func (s *ConfigTableAdminService) ReloadConfigTable(ctx context.Context, req *configv1.ReloadConfigTableRequest) (*configv1.ReloadConfigTableResponse, error) {
	if caller := callerID(ctx); caller != 0 {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "ReloadConfigTable",
			"reason", "player_jwt_not_allowed", "player_id", caller,
			"expect_version", req.GetExpectVersion())
		return &configv1.ReloadConfigTableResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY,
			Detail: "player-facing calls are not allowed"}, nil
	}
	currentVersion := func() uint64 {
		if tb := s.store.Tables(); tb != nil {
			return tb.Version
		}
		return 0
	}

	res, err := s.store.Load(s.activeDir, req.GetExpectVersion())
	if err != nil {
		plog.With(ctx).Errorw("msg", "configtable_reload_failed", "dir", s.activeDir,
			"expect_version", req.GetExpectVersion(), "err", err)
		return &configv1.ReloadConfigTableResponse{
			Code:          commonv1.ErrCode_ERR_INVALID_STATE,
			ActiveVersion: currentVersion(),
			Detail:        err.Error(),
		}, nil
	}
	for _, w := range res.Warnings {
		plog.With(ctx).Warnw("msg", "configtable_reload_warning", "warning", w)
	}
	detail := ""
	if !res.Reloaded {
		detail = "version unchanged, no-op"
	} else if len(res.Warnings) > 0 {
		detail = strings.Join(res.Warnings, "; ")
	}
	plog.With(ctx).Infow("msg", "configtable_reloaded", "version", res.Version, "reloaded", res.Reloaded)
	return &configv1.ReloadConfigTableResponse{
		Code:          commonv1.ErrCode_OK,
		ActiveVersion: res.Version,
		Reloaded:      res.Reloaded,
		Detail:        detail,
	}, nil
}
