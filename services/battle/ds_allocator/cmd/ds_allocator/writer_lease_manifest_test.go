package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// stripYAMLComments 去掉整行注释与行尾注释。清单断言必须只看**生效字段**:
// 注释里出现 "type: Recreate" / "/healthz/writer" 这类字样是常态(本文件的两步升级
// 说明就写了),让注释能满足或触发断言等于门禁形同虚设。
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

// dsAllocatorDeploymentSection 截出 services.yaml 里 ds-allocator Deployment 的整段
// (已剥离注释)。
func dsAllocatorDeploymentSection(t *testing.T) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test source path")
	}
	manifestPath := filepath.Clean(filepath.Join(filepath.Dir(testFile), "..", "..", "..", "..", "..",
		"deploy", "k8s", "services", "services.yaml"))
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	const deploymentMarker = "metadata: { name: ds-allocator, namespace: pandora, labels: { app: ds-allocator } }"
	start := strings.Index(string(raw), deploymentMarker)
	if start < 0 {
		t.Fatal("ds-allocator Deployment not found")
	}
	section := string(raw)[start:]
	if end := strings.Index(section, "\n---"); end >= 0 {
		section = section[:end]
	}
	return stripYAMLComments(section)
}

// TestDsAllocatorSurvivesRestartWithoutKillingLiveBattles 是 2026-07-29 事故的机械回归。
//
// 事故链:ds-allocator replicas=1 + Recreate,整进程被 capability 门控、失租即 os.Exit(1)
// → 任何重启都让 Heartbeat 整体不可用 → Battle DS 在 20s(pkg/placement.DSFenceLeaseMaxSeconds)
// 内收不到凭据绑定 ACK → 自我 fencing 踢掉全部在场玩家。实测恢复 160s ≈ 8× 租约。
//
// 本测试守护「重启不打断对局」所依赖的三件耦合事实,任一被悄悄改回都必须失败:
//  1. 多副本 + RollingUpdate(maxUnavailable=0),且有 PDB 兜住节点排空;
//  2. 单扫描者由运行时选举保证(writerlease),不是由"同一时刻只有一个进程"保证;
//  3. readiness 不绑定写者身份 —— 热备副本必须继续服务 Heartbeat,否则等于没有多副本。
func TestDsAllocatorSurvivesRestartWithoutKillingLiveBattles(t *testing.T) {
	section := dsAllocatorDeploymentSection(t)

	if strings.Contains(section, "replicas: 1") {
		t.Fatalf("单副本 = 重启即全服 Heartbeat 断流,Battle DS 20s 后集体踢人:\n%s", section)
	}
	if !strings.Contains(section, "type: RollingUpdate") ||
		!strings.Contains(section, "maxUnavailable: 0") {
		t.Fatalf("ds-allocator 必须 RollingUpdate maxUnavailable=0(升级全程恒有副本在服务):\n%s", section)
	}
	// 运维面端口必须真的声明,否则 sum(pandora_ds_allocator_writer_held)==0 这条
	// 「长期无人扫描」告警在集群内没有抓取路径(同 hub_allocator R11 P0-2)。
	if !strings.Contains(section, "containerPort: 21020") {
		t.Fatal("运维面 HTTP 21020 必须声明,否则集群内抓不到写者指标")
	}

	mainSrc, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(mainSrc)
	if !strings.Contains(source, "writerlease.Start(") ||
		!strings.Contains(source, "uc.SetSweepWriterLease(") {
		t.Fatal("多副本下心跳超时扫描必须由 writerlease 选举串行化,不能靠部署策略")
	}
	if !strings.Contains(source, "writerHealth.Set(writerLease, writerMode)") {
		t.Fatal("长期无主必须可观测(/healthz/writer + 指标),否则补偿链停摆无人知道")
	}
	// 热备语义:失去领导权的副本仍然服务 Heartbeat/AllocateBattle。把 /healthz/writer
	// 接进 readiness 会在滚动升级时死锁,并把"扫描降级"放大成"整服零 allocator 端点"。
	if strings.Contains(section, "/healthz/writer") {
		t.Fatal("/healthz/writer 是观测端点,不得进入探针")
	}
}

// TestDsAllocatorDeployStrategyAnnotationMatchesSpec:进程看不到 spec.strategy,
// 只能读注入的 annotation。若 annotation 与真实 strategy 漂移,启动期那道
// 「RollingUpdate × 非 enforce」的 fail-closed 门就成了纸面上的。这里把三者钉死。
func TestDsAllocatorDeployStrategyAnnotationMatchesSpec(t *testing.T) {
	section := dsAllocatorDeploymentSection(t)
	declared := ""
	switch {
	case strings.Contains(section, "type: RollingUpdate"):
		declared = "RollingUpdate"
	case strings.Contains(section, "type: Recreate"):
		declared = "Recreate"
	default:
		t.Fatal("ds-allocator Deployment must declare an explicit strategy type")
	}
	if !strings.Contains(section, `pandora.dev/deploy-strategy: "`+declared+`"`) {
		t.Fatalf("pandora.dev/deploy-strategy 必须与 spec.strategy.type(%s)逐字一致,"+
			"运行时门禁读的是这个 annotation,漂移即门禁失效:\n%s", declared, section)
	}
	if !strings.Contains(section, "PANDORA_DEPLOY_STRATEGY") {
		t.Fatal("strategy annotation 必须以 PANDORA_DEPLOY_STRATEGY 注入,启动期门禁才看得到")
	}
	mainSrc, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(mainSrc)
	if !strings.Contains(source, `os.Getenv("PANDORA_DEPLOY_STRATEGY")`) ||
		!strings.Contains(source, "ds_writer_lease_rollingupdate_without_enforce") {
		t.Fatal("main 必须对 RollingUpdate × writer_lease_mode!=enforce fail-closed")
	}
}

// TestDsAllocatorPodDisruptionBudgetKeepsOneServing:kubectl drain / 节点维护若把两个
// 副本一起赶走,Heartbeat 断流 >20s 的后果与单副本重启完全相同。
func TestDsAllocatorPodDisruptionBudgetKeepsOneServing(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test source path")
	}
	manifestPath := filepath.Clean(filepath.Join(filepath.Dir(testFile), "..", "..", "..", "..", "..",
		"deploy", "k8s", "services", "services.yaml"))
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	const pdbMarker = "kind: PodDisruptionBudget"
	text := string(raw)
	found := false
	for idx := strings.Index(text, pdbMarker); idx >= 0; {
		section := text[idx:]
		if end := strings.Index(section, "\n---"); end >= 0 {
			section = section[:end]
		}
		section = stripYAMLComments(section)
		if strings.Contains(section, "name: ds-allocator") && strings.Contains(section, "minAvailable: 1") {
			found = true
			break
		}
		next := strings.Index(text[idx+len(pdbMarker):], pdbMarker)
		if next < 0 {
			break
		}
		idx += len(pdbMarker) + next
	}
	if !found {
		t.Fatal("ds-allocator 必须有 minAvailable:1 的 PodDisruptionBudget,否则节点排空会同时驱逐两个副本")
	}
}
