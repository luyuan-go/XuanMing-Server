// allocation_ledger.go — battle allocation 台账(2026-08-03,孤儿清扫 P0 整改)。
//
// 动机(对抗审查确认的 P0「权威视图与被清扫集群零绑定」):孤儿 Allocated GS 对账清扫
// 的安全声明依赖一个此前未被机制强制的前提——清扫进程读到的 Redis 权威,与签票方/
// 心跳处理方读的是同一份。若一个配置漂移的副本(第二套部署、宿主残留进程、Redis
// failover 到空实例)读到健康但空的 Redis,`RangeActiveBattles` 会成功返回空集而非
// 报错,防误删①(证据不可得不删)不触发,整个集群的载人 Allocated GS 会在阈值后被
// 机械化全删。
//
// 台账把「我读的权威」与「我要删的 GS」绑定:每次 battle 分配在 ClaimBattle 成功后
// 把 allocation_id 记入本 ZSET(score=毫秒时间戳);孤儿清扫删除一台 GS 前必须证明
// 其 pandora.dev/allocation-id label 的值**曾在本权威中出现过**(ZSCORE 命中)。
// 空/错配 Redis 的台账必然为空 ⇒ 一台都删不掉(fail-closed),而本权威分配出去后
// 泄漏的 GS 台账必然有记录 ⇒ 照常回收。
//
// 有界性(§9.24 纪律,Redis 侧):条目数 = 保留期内的分配次数;孤儿清扫每轮
// ZREMRANGEBYSCORE 清掉超过保留期(biz.orphanGSLedgerRetention,7 天)的条目。
// 保留期 ≫ 孤儿观察阈值(10min)即满足功能;取 7 天是给「泄漏很久才被注意到」的
// 场景留余量。写失败不阻断分配(可用性优先):代价只是该次分配若泄漏,清扫因台账
// 查无而保留不删——方向安全,宁可占位。
package data

import (
	"context"
	"errors"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// allocationLedgerKey 是 battle allocation 台账 ZSET(member=allocation_id,score=ms)。
const allocationLedgerKey = "pandora:ds:allocation_ledger"

// RecordAllocationLedger 把一次分配的 allocation_id 记入台账。幂等(重复 ZADD 只刷 score)。
func (r *RedisBattleRepo) RecordAllocationLedger(ctx context.Context, allocationID string, atMs int64) error {
	if allocationID == "" {
		return nil
	}
	return r.rdb.ZAdd(ctx, allocationLedgerKey,
		redis.Z{Score: float64(atMs), Member: allocationID}).Err()
}

// AllocationLedgerContains 查询 allocation_id 是否曾在本权威中出现过。
// 查询错误必须如实上抛(调用方 fail-closed 保留候选),不得把错误冒充成 false。
func (r *RedisBattleRepo) AllocationLedgerContains(ctx context.Context, allocationID string) (bool, error) {
	if allocationID == "" {
		return false, nil
	}
	err := r.rdb.ZScore(ctx, allocationLedgerKey, allocationID).Err()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// PruneAllocationLedger 清掉 score 早于 beforeMs 的台账条目,返回清除数(有界性闸)。
func (r *RedisBattleRepo) PruneAllocationLedger(ctx context.Context, beforeMs int64) (int64, error) {
	return r.rdb.ZRemRangeByScore(ctx, allocationLedgerKey,
		"-inf", strconv.FormatInt(beforeMs, 10)).Result()
}
