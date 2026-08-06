package scenario

import (
	"os"
	"path/filepath"
	"testing"
)

// Default 必须默认开 AutoConfirmMatch,与 matchmaker-dev.yaml 的 auto_confirm_match: true 一致;
// 否则 VU 会在 auto-confirm 后端上抢发 ConfirmMatch 造成竞态报错。
func TestDefault_AutoConfirmMatchOn(t *testing.T) {
	if !Default().AutoConfirmMatch {
		t.Fatal("Default().AutoConfirmMatch 应为 true(对齐 matchmaker-dev auto_confirm_match)")
	}
}

// JSON 显式覆盖 auto_confirm_match 应生效(支持手动确认模式联调)。
func TestLoad_AutoConfirmMatchOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cfg.json")
	const body = `{
	  "targets": {"login": "127.0.0.1:20001"},
	  "vu_count": 10,
	  "ramp_seconds": 1,
	  "steady_seconds": 1,
	  "action_interval_ms": 100,
	  "auto_confirm_match": false
	}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("写临时配置失败: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AutoConfirmMatch {
		t.Fatal("auto_confirm_match: false 应覆盖默认 true")
	}
}
