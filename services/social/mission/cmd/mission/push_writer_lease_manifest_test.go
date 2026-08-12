package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// push_writer_lease_manifest_test.go — 推送发布器单写者的**清单契约**回归(2026-08-11)。
//
// 守护的不变量:mission_push_outbox 是全局未分区表,按 id 序整表 FIFO 取行,属
// CLAUDE.md §9.21 点名要串行化的「作用于同一未分区权威的单写者循环」。两个副本同时
// 发布时,各自持有一份内存快照、投递顺序交错,而 MissionUpdateEvent.progressed 是
// **逐任务全量快照**(不是增量),后到即覆盖 —— 玩家 UI 进度条会从 7/10 退回 3/10。
// 事件里没有任何 revision 可供客户端判旧(ts_ms 是 event 级、跨副本各自墙钟,
// docs/design/protocol-ordering-rules.md §5-B 明令不得只靠它判重)。
//
// 「同时只有一个发布器」由**两道闸**保证,本文件把两道闸的耦合事实钉死:
//  1. 产物侧:集群配置 push_writer_lease.mode=enforce(gen_cluster_config.ps1 机械改写);
//  2. 进程侧:main.go 读 Deployment 注入的 PANDORA_DEPLOY_STRATEGY,
//     RollingUpdate × 非 enforce 直接 fail-closed 退出。
//
// 注意 replicas:1 **不**意味着单进程:Deployment 是 RollingUpdate(§9.16/§9.21 不停服
// 硬要求,Recreate 被审计否决),maxSurge 让每次发版都有新旧两 Pod 并存窗口。

// stripYAMLComments 去掉整行注释与行尾注释。清单断言必须只看**生效字段**:
// 注释里出现 "mode: enforce" / "RollingUpdate" 这类字样是常态(本次改动的说明就写了),
// 让注释能满足断言等于门禁形同虚设。
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

// missionDeploymentSection 截出 services.yaml 里 mission Deployment 的整段(已剥离注释)。
func missionDeploymentSection(t *testing.T) string {
	t.Helper()
	manifestPath := filepath.Join(repoRoot(t), "deploy", "k8s", "services", "services.yaml")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	const marker = "metadata: { name: mission, namespace: pandora, labels: { app: mission } }"
	start := strings.Index(string(raw), marker)
	if start < 0 {
		t.Fatal("mission Deployment not found in services.yaml")
	}
	section := string(raw)[start:]
	if end := strings.Index(section, "\n---"); end >= 0 {
		section = section[:end]
	}
	return stripYAMLComments(section)
}

// TestMissionDeploymentPinsPushWriterLeaseGate 守护清单侧三件耦合事实。
func TestMissionDeploymentPinsPushWriterLeaseGate(t *testing.T) {
	section := missionDeploymentSection(t)

	// ① 必须是 RollingUpdate(不停服硬要求)。若哪天真改成 Recreate,本断言会红 ——
	// 那时应当同步复核「单发布者是否还需要选举」,而不是默默删掉这条测试。
	if !strings.Contains(section, "RollingUpdate") {
		t.Fatal("mission Deployment 必须是 RollingUpdate(§9.16/§9.21 不停服硬要求)")
	}
	if !strings.Contains(section, "maxUnavailable: 0") {
		t.Fatal("maxUnavailable 必须为 0:滚动期不允许出现零可用副本")
	}

	// ② strategy annotation 必须与真实 strategy.type 逐字一致 —— 进程看不到
	// spec.strategy,只能读这个注入值,注解漂了门禁就守错了对象。
	if !strings.Contains(section, `pandora.dev/deploy-strategy: "RollingUpdate"`) {
		t.Fatal("必须有 pandora.dev/deploy-strategy 注解且与 spec.strategy.type 一致")
	}

	// ③ 注解必须以 PANDORA_DEPLOY_STRATEGY 注入 env,启动期门禁才看得到。
	if !strings.Contains(section, "PANDORA_DEPLOY_STRATEGY") {
		t.Fatal("strategy annotation 必须以 PANDORA_DEPLOY_STRATEGY 注入 env,启动期门禁才看得到")
	}
}

// TestMissionMainFailsClosedOnRollingUpdateWithoutEnforce 守护进程侧那道闸真的还在。
//
// 清单断言只能证明"注解与 env 在",证明不了"进程真的会因此退出"。这里读 main.go 源码
// 钉住门禁三要素同时存在;删掉任何一个都会红。
func TestMissionMainFailsClosedOnRollingUpdateWithoutEnforce(t *testing.T) {
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

// TestClusterMissionConfigEnforcesPushWriterLease 守护**产物**侧那道闸。
//
// 光有进程门禁不够:门禁只在受管 k8s 内生效,而产物是运维实际读的东西;
// gen_cluster_config.ps1 若哪天漏掉 mission,产物会继承 dev 的 mode:"off",
// Pod 起来就直接 fail-closed 退出(比默默并发发布好,但仍是一次发布事故)。
func TestClusterMissionConfigEnforcesPushWriterLease(t *testing.T) {
	path := filepath.Join(repoRoot(t), "run", "cluster", "etc", "mission.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("集群产物未生成(跑 tools/scripts/gen_cluster_config.ps1 后再验): %v", err)
	}
	out := stripYAMLComments(string(raw))
	if !strings.Contains(out, `mode: "enforce"`) {
		t.Fatal("集群产物 mission.yaml 的 push_writer_lease.mode 必须是 enforce(不得继承 dev 的 off)")
	}
	if !strings.Contains(out, "etcd_endpoints:") {
		t.Fatal("enforce 必须带 etcd_endpoints,否则进程启动即 fail-closed")
	}
}
