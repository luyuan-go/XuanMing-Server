package scenario

import (
	"os"
	"path/filepath"
	"testing"
)

// Default 必须默认关 AutoConfirmMatch,与 matchmaker-dev.yaml 的 auto_confirm_match: false 一致
// (2026-08-17 LoL 式流程:确认期是「带缺席者开局」的主防线,VU 必须真实走 ConfirmMatch)。
// 方向安全性:手动档 VU 打在 auto-confirm 后端上只会产生可容忍的竞态报错;
// 反过来(auto 档 VU 打在手动后端上)会让所有 match 15s 超时判 FAILED,漏斗全断。
func TestDefault_AutoConfirmMatchOff(t *testing.T) {
	if Default().AutoConfirmMatch {
		t.Fatal("Default().AutoConfirmMatch 应为 false(对齐 matchmaker-dev auto_confirm_match)")
	}
}

// JSON 显式覆盖 auto_confirm_match 应生效(压测环境显式开自动确认时用)。
func TestLoad_AutoConfirmMatchOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cfg.json")
	const body = `{
	  "targets": {"login": "127.0.0.1:20001"},
	  "vu_count": 10,
	  "ramp_seconds": 1,
	  "steady_seconds": 1,
	  "action_interval_ms": 100,
	  "auto_confirm_match": true
	}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("写临时配置失败: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.AutoConfirmMatch {
		t.Fatal("auto_confirm_match: true 应覆盖默认 false")
	}
}
