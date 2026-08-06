package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/biz"
)

// TestKubernetesDeploymentRollingUpdateRequiresWriterLease:R9 P0-7 起单写者约束由
// 运行时写者继任租约(writerlease)+ 存储级 fencing 保证,部署策略允许 RollingUpdate
// (maxUnavailable=0 消除升级停机)。本测试守护两个耦合不变量:
//  1. manifest 必须是 maxUnavailable: 0 的 RollingUpdate(不允许悄悄退回 Recreate
//     停机,也不允许放开 maxUnavailable 造成升级空窗);
//  2. main.go 必须真的装配 writerlease 且注入 usecase/repo——没有租约的
//     RollingUpdate 就是 INC-20260722-004 R9 P0-7 的双写者事故窗口。
func TestKubernetesDeploymentRollingUpdateRequiresWriterLease(t *testing.T) {
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
	const deploymentMarker = "metadata: { name: hub-allocator, namespace: pandora, labels: { app: hub-allocator } }"
	start := strings.Index(string(raw), deploymentMarker)
	if start < 0 {
		t.Fatal("hub-allocator Deployment not found")
	}
	section := string(raw)[start:]
	if end := strings.Index(section, "\n---"); end >= 0 {
		section = section[:end]
	}
	if !strings.Contains(section, "type: RollingUpdate") ||
		!strings.Contains(section, "maxUnavailable: 0") {
		t.Fatalf("hub-allocator must roll with RollingUpdate maxUnavailable=0 (writer succession lease):\n%s", section)
	}
	mainSrc, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(mainSrc)
	if !strings.Contains(source, "writerlease.Start(") ||
		!strings.Contains(source, "uc.SetWriterFence(") ||
		!strings.Contains(source, "repo.SetWriterFence(") {
		t.Fatal("RollingUpdate is only safe with the writer succession lease wired into usecase and repos (R9 P0-7)")
	}
	// R10 复审 P0-4:继任者 fence 推扫必须是**接流前硬门**(当选后、宣告持有前跑完),
	// 不能退回后台 sweep tick 上的懒执行——那会留下"已接写、继任未完成"的双写窗口。
	if !strings.Contains(source, "leaseCfg.OnElected = func(") ||
		!strings.Contains(source, "repo.AdvanceWriterFencesForToken(ctx, token)") {
		t.Fatal("writer lease must gate writability on the successor fence sweep (R10 P0-4)")
	}
	// R10 复审 P0-2:长期无主必须可观测(/healthz/writer;**不接 readiness**,热备语义)。
	if !strings.Contains(source, "writerHealth.Set(writerLease, writerMode)") {
		t.Fatal("writer lease health must be exposed for alerting (R10 P0-2)")
	}
	if strings.Contains(section, "readinessProbe") && strings.Contains(section, "/healthz/writer") {
		t.Fatal("/healthz/writer is an observability endpoint; wiring it into readiness deadlocks rolling upgrades")
	}
	// R11 复审 P0-5:机械门禁必须真的能生效。进程看不到 spec.strategy,只能读注入的
	// annotation;若 annotation 与真实 strategy 漂移,门禁就成了纸面上的。这里把两者钉死。
	declared := ""
	switch {
	case strings.Contains(section, "type: RollingUpdate"):
		declared = "RollingUpdate"
	case strings.Contains(section, "type: Recreate"):
		declared = "Recreate"
	default:
		t.Fatal("hub-allocator Deployment must declare an explicit strategy type")
	}
	if !strings.Contains(section, `pandora.dev/deploy-strategy: "`+declared+`"`) {
		t.Fatalf("pandora.dev/deploy-strategy annotation must match spec.strategy.type (%s);"+
			" the runtime double-write guard reads the annotation, so drift disables it:\n%s", declared, section)
	}
	if !strings.Contains(section, "PANDORA_DEPLOY_STRATEGY") {
		t.Fatal("the strategy annotation must be injected as PANDORA_DEPLOY_STRATEGY for the startup guard to see it")
	}
	if !strings.Contains(source, `os.Getenv("PANDORA_DEPLOY_STRATEGY")`) ||
		!strings.Contains(source, "hub_writer_lease_rollingupdate_without_enforce") {
		t.Fatal("main must fail-closed on RollingUpdate × writer_lease_mode!=enforce (R11 P0-5)")
	}
	// R11 复审 P0-2:长期无主必须能被抓取端自动采集,不能只有 JSON 健康页。
	if !strings.Contains(section, "containerPort: 21021") {
		t.Fatal("the ops HTTP port must be declared so in-cluster scraping can reach the writer metrics (R11 P0-2)")
	}
}

func TestHubWriterStagesCapabilityButCannotServeBeforePolicyV3(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	feature := strings.Index(source, `"hub-successor-lease-v1"`)
	gate := strings.Index(source, "fence.RequiredPolicyGeneration() != dsauthfence.RequiredPolicyGenerationV3")
	sweep := strings.Index(source, "go uc.RunHeartbeatSweep")
	serve := strings.Index(source, "app.Run()")
	if feature < 0 || gate < 0 || sweep < 0 || serve < 0 || gate > sweep || gate > serve {
		t.Fatalf("Hub must advertise V3 support but block all background/RPC writers while staged: feature=%d gate=%d sweep=%d serve=%d",
			feature, gate, sweep, serve)
	}
	stagingBlock := source[gate:sweep]
	if !strings.Contains(stagingBlock, "<-fence.Lost()") || !strings.Contains(stagingBlock, "os.Exit(0)") {
		t.Fatal("staged Hub must wait for the V2->V3 watch event and restart before serving")
	}
}

func TestHubTicketSignerB1RS256CompleteBinding(t *testing.T) {
	privatePEM, publicKey, kid, err := auth.GenerateDSTicketKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewDSTicketSigner(auth.DSTicketSignerConfig{PrivateKeyPEM: privatePEM, ActiveKid: kid})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &hubTicketSigner{v2: signer}
	token, _, err := adapter.SignHubTicket(1001, 7, biz.HubTicketBinding{
		PodName: "hub-canary-1", InstanceUID: "uid-hub-canary", ProtocolEpoch: 3,
		HubAssignmentID: "assignment-1", ReleaseTrack: auth.ReleaseTrackCanary,
	})
	if err != nil {
		t.Fatal(err)
	}
	alg, err := auth.DSTicketAlgorithm(token)
	if err != nil || alg != "RS256" {
		t.Fatalf("alg=%q err=%v", alg, err)
	}
	jwks, err := auth.MarshalDSTicketJWKS(1, kid, publicKey)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewDSTicketVerifier(auth.DSTicketVerifierConfig{JWKS: jwks})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.DstVer != auth.DSTicketVersion2 || claims.DSType != string(auth.DSTypeHub) ||
		claims.PlayerID() != 1001 || claims.DSPodName != "hub-canary-1" ||
		claims.DSInstanceUID != "uid-hub-canary" || claims.DSInstanceEpoch != 3 ||
		claims.HubAssignmentID != "assignment-1" || claims.ReleaseTrack != auth.ReleaseTrackCanary {
		t.Fatalf("claims=%+v", claims)
	}

	if _, _, err := adapter.SignHubTicket(1001, 7, biz.HubTicketBinding{
		PodName: "hub-1", InstanceUID: "uid-1", ProtocolEpoch: 1, HubAssignmentID: "assignment-2",
	}); err == nil {
		t.Fatal("missing release_track must fail closed")
	}
}
