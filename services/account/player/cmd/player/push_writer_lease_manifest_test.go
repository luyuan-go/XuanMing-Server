package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// push_writer_lease_manifest_test.go — 推送出箱发布器单写者的**清单契约**回归(2026-08-11)。
//
// 守护的不变量:player_push_outbox 是全局未分区表,发布器按 id 升序整表 FIFO 取行
// (experience_repo.go FetchPushOutbox),属 CLAUDE.md §9.21「作用于同一未分区权威的
// 单写者循环」。PlayerExperienceEvent 携带的是**绝对值快照**(level / exp_in_level /
// is_max_level,不是增量),两个副本交错投递会让旧快照后到并覆盖新的 —— 玩家看到
// 等级、经验条**倒退**。事件里没有 revision;ts_ms 是各副本墙钟,
// docs/design/protocol-ordering-rules.md §5-B 明令不得只靠它判重。
//
// 与 mission 同款两道闸:①产物侧 push_writer_lease.mode=enforce(gen_cluster_config.ps1
// 机械改写);②进程侧 main.go 读 PANDORA_DEPLOY_STRATEGY,RollingUpdate × 非 enforce
// fail-closed 退出。replicas:1 **不**意味着单进程:maxSurge 让每次发版都有并存窗口。

// stripYAMLComments 去掉整行注释与行尾注释 —— 断言必须只看生效字段,
// 否则注释里出现 "mode: enforce" 就能满足门禁,等于形同虚设。
func stripYAMLComments(section string) string {
	var b strings.Builder
	for _, line := range strings.Split(section, "\n") {
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = line[:idx]
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(testFile), "..", "..", "..", "..", ".."))
}

func playerDeploymentSection(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "deploy", "k8s", "services", "services.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	const marker = "metadata: { name: player, namespace: pandora, labels: { app: player } }"
	start := strings.Index(string(raw), marker)
	if start < 0 {
		t.Fatal("player Deployment not found in services.yaml")
	}
	section := string(raw)[start:]
	if end := strings.Index(section, "\n---"); end >= 0 {
		section = section[:end]
	}
	return stripYAMLComments(section)
}

// TestPlayerDeploymentPinsPushWriterLeaseGate 守护清单侧三件耦合事实。
func TestPlayerDeploymentPinsPushWriterLeaseGate(t *testing.T) {
	section := playerDeploymentSection(t)
	if !strings.Contains(section, "RollingUpdate") {
		t.Fatal("player Deployment 必须是 RollingUpdate(§9.16/§9.21 不停服硬要求)")
	}
	if !strings.Contains(section, "maxUnavailable: 0") {
		t.Fatal("maxUnavailable 必须为 0:滚动期不允许出现零可用副本")
	}
	if !strings.Contains(section, `pandora.dev/deploy-strategy: "RollingUpdate"`) {
		t.Fatal("必须有 pandora.dev/deploy-strategy 注解且与 spec.strategy.type 一致")
	}
	if !strings.Contains(section, "PANDORA_DEPLOY_STRATEGY") {
		t.Fatal("strategy annotation 必须以 PANDORA_DEPLOY_STRATEGY 注入 env,启动期门禁才看得到")
	}
}

// TestPlayerMainFailsClosedOnRollingUpdateWithoutEnforce 守护进程侧那道闸真的还在。
func TestPlayerMainFailsClosedOnRollingUpdateWithoutEnforce(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test source path")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(testFile), "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, want := range []string{
		`os.Getenv("PANDORA_DEPLOY_STRATEGY")`,
		`strings.EqualFold(deployStrategy, "RollingUpdate")`,
		`conf.PushWriterLeaseEnforce`,
		`writerlease.Start(`,
		`SetPushWriterLease(`,
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("main.go 必须保留推送发布器单写者门禁要素 %q;缺了它 RollingUpdate 滚动窗口会出现并发发布器", want)
		}
	}
}

// TestClusterPlayerConfigEnforcesPushWriterLease 守护**产物**侧那道闸。
func TestClusterPlayerConfigEnforcesPushWriterLease(t *testing.T) {
	path := filepath.Join(repoRoot(t), "run", "cluster", "etc", "player.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("集群产物未生成(跑 tools/scripts/gen_cluster_config.ps1 后再验): %v", err)
	}
	out := stripYAMLComments(string(raw))
	if !strings.Contains(out, `mode: "enforce"`) {
		t.Fatal("集群产物 player.yaml 的 push_writer_lease.mode 必须是 enforce(不得继承 dev 的 off)")
	}
	if !strings.Contains(out, "etcd_endpoints:") {
		t.Fatal("enforce 必须带 etcd_endpoints,否则进程启动即 fail-closed")
	}
}
