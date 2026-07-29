// Package svc 提供 Pandora 服务的通用 ServiceContext 模板。
//
// 各业务服务的 internal/svc/servicecontext.go 嵌入 BaseContext + 加业务字段。
//
// 用法:
//
//	type ServiceContext struct {
//	    *svc.BaseContext             // 公共:Redis / RedisLocker / KillSwitch
//	    PlayerLocatorClient plpb.PlayerLocatorClient  // 业务私有
//	    MyBusinessHandler   *myHandler
//	}
//
// ⚠️ 本模板**刻意不提供 snowflake 发号器**。发号器必须由服务 main.go 经
// pkg/snowflake/etcdnode.MustProvideSnowflake(N) 装配:它一处收敛了 static / etcd 两态
// 与失租 fencing 退出契约,而在这里 snowflake.NewNode(node_id) 只有 static 一条路
// ——多副本(含每次滚更的新旧并存窗口)会双活发逐位相同的重号,且失租无人 fencing。
// 详见 docs/design/infra.md §8.1 / §8.2。
//
//	func NewServiceContext(c config.Config) *ServiceContext {
//	    base := svc.MustNewBaseContext(c.Base)
//	    return &ServiceContext{
//	        BaseContext:         base,
//	        PlayerLocatorClient: plpb.NewPlayerLocatorClient(grpcclient.MustNewClient(...).Conn()),
//	    }
//	}
package svc

import (
	"context"
	"fmt"
	"time"

	klog "github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/killswitch"
	"github.com/luyuancpp/pandora/pkg/redislock"
	"github.com/luyuancpp/pandora/pkg/redisx"
)

// BaseContext 是所有 Pandora 服务共享的运行时上下文。
type BaseContext struct {
	// RedisClient 是该服务的主 Redis 客户端(对应 config.Node.RedisClient)。
	//
	// 类型为 redis.UniversalClient 接口(非 *redis.Client),按 config.RedisConf 的
	// addrs / master_name 自动选型:单实例 / Sentinel 主从 / Cluster 分片。留空 = 单实例,
	// 行为与旧版完全一致。详见 docs/design/scale-cellular-20m.md 的单 Cell 扩容口径。
	RedisClient redis.UniversalClient

	// Locker 是 Redis 分布式锁实例(用 pandora:lock: 前缀)。
	Locker *redislock.RedisLocker

	// KillSwitch 是 RPC 级临时关停的开关源,进程退出时需 Close。
	KillSwitch killswitch.Source

	// Cfg 是公共配置的副本。
	Cfg config.Base
}

// MustNewBaseContext 用 config.Base 构造 BaseContext。失败 panic。
//
// 调用前必须已经 log.Setup() 过(初始化 logx)。
//
// Redis 为全服强依赖:Node.RedisClient 的 host 与 addrs 同时为空视为漏配,直接 panic;
// 构造后做一次带超时的 Ping,连不上立即 panic,避免服务带着死 Redis 启动、首条命令才暴露。
func MustNewBaseContext(c config.Base) *BaseContext {
	// 0. 校验 Redis endpoint 已配置(单实例填 host,Sentinel / Cluster 填 addrs)。
	rc := c.Node.RedisClient
	if rc.Host == "" && len(rc.Addrs) == 0 {
		panic("svc.MustNewBaseContext: redis endpoint required, set node.redis_client.host (single) or node.redis_client.addrs (sentinel/cluster)")
	}

	// 1. Redis client(UniversalClient:单实例 / Sentinel / Cluster 由配置驱动)
	rdb := redisx.NewUniversalClient(rc)

	// 1.1 Ping 探活:强依赖不可降级,连不上立即 panic(对齐各服务 main.go 启动校验)。
	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancel()
		_ = rdb.Close()
		panic(fmt.Sprintf("svc.MustNewBaseContext: redis ping failed host=%s addrs=%v: %v", rc.Host, rc.Addrs, err))
	}
	cancel()

	// 2. Locker
	lk := redislock.NewRedisLocker(rdb)

	// 3. Kill-Switch(RPC 级临时关停)。fail-open:配置缺失 / 源建不起来都不阻断启动。
	ks := mustSetupKillSwitch(c.KillSwitch)

	klog.Infof("[svc] BaseContext ready node=%d redis=%s", c.Node.NodeId, rc.Host)

	return &BaseContext{
		RedisClient: rdb,
		Locker:      lk,
		KillSwitch:  ks,
		Cfg:         c,
	}
}

// mustSetupKillSwitch 把 config.KillSwitchConf 翻译成 killswitch.Config 并启动开关源。
//
// FailClosed=false(默认)时:即便源建不起来也只 Warn 放行,返回非 nil 的 noop Source;
// FailClosed=true 时:源建不起来直接 panic(要求开关系统必须在线的场景)。
func mustSetupKillSwitch(c config.KillSwitchConf) killswitch.Source {
	cfg := killswitch.Config{
		Enabled:         c.Enabled,
		Source:          c.Source,
		FilePath:        c.FilePath,
		EtcdEndpoints:   c.EtcdEndpoints,
		EtcdPrefix:      c.EtcdPrefix,
		EtcdDialTimeout: c.EtcdDialTimeout.Std(),
		FailOpen:        !c.FailClosed,
	}
	src, err := killswitch.Setup(cfg)
	if err != nil {
		// 只有 FailClosed=true 时 Setup 才会返回 err。
		panic("svc.MustNewBaseContext killswitch setup: " + err.Error())
	}
	return src
}

// Close 关闭 BaseContext 下属资源。业务的 ServiceContext.Close() 应调用本方法。
func (b *BaseContext) Close() error {
	if b.KillSwitch != nil {
		if err := b.KillSwitch.Close(); err != nil {
			klog.Errorf("[svc] killswitch close: %v", err)
		}
	}
	if b.RedisClient != nil {
		if err := b.RedisClient.Close(); err != nil {
			klog.Errorf("[svc] redis close: %v", err)
		}
	}
	return nil
}
