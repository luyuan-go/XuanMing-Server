package conf

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
)

// TestLocalDSLauncherDefaults 锁住「加 editor 模式不影响原有链路」的不变量:
// 老配置(不写 launcher)必须归一化成 packaged,超时值维持 15s/10s 原样。
func TestLocalDSLauncherDefaults(t *testing.T) {
	os.Unsetenv("PANDORA_DS_LAUNCHER")
	os.Unsetenv("PANDORA_DS_UPROJECT")

	for _, tc := range []struct {
		name string
		in   string
	}{
		{"空值归一化为 packaged", ""},
		{"未知值归一化为 packaged", "listen"},
		{"显式 packaged", "packaged"},
		{"大小写与空白容错", "  PACKAGED  "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{}
			c.Mode = ModeLocal
			c.LocalDS.Launcher = tc.in
			c.Defaults()
			if c.LocalDS.Launcher != LauncherPackaged {
				t.Fatalf("launcher: got %q want %q", c.LocalDS.Launcher, LauncherPackaged)
			}
			if got := time.Duration(c.Allocator.HeartbeatTimeout); got != 15*time.Second {
				t.Fatalf("packaged 的心跳超时不应被改动: got %v want 15s", got)
			}
			if got := time.Duration(c.Allocator.ReadyWaitTimeout); got != 10*time.Second {
				t.Fatalf("packaged 的 ready 等待不应被改动: got %v want 10s", got)
			}
		})
	}

	t.Run("editor 大小写容错", func(t *testing.T) {
		c := &Config{}
		c.Mode = ModeLocal
		c.LocalDS.Launcher = " Editor "
		c.Defaults()
		if c.LocalDS.Launcher != LauncherEditor {
			t.Fatalf("launcher: got %q want %q", c.LocalDS.Launcher, LauncherEditor)
		}
	})
}

// TestLocalDSEditorWidensTimeouts:editor 形态要加载一大批编辑器模块、读未 cook 的散装资产
// (首次进新图还可能现场构网格/贴图的 DDC),首次进图可能几十秒到几分钟,沿用 15s/10s 会在
// DS 就绪前就被判定失联/超时回收。(注:并不包括编 shader —— -server 下 IsRunningDedicatedServer()
// 为真,引擎跳过全局着色器与材质着色器编译;要编 shader 的是 listen server / PIE 那类会出画面的形态。)
// 只在 mode=local && launcher=editor && 未显式配置时放宽;显式值与其它模式一律不动。
func TestLocalDSEditorWidensTimeouts(t *testing.T) {
	os.Unsetenv("PANDORA_DS_LAUNCHER")

	t.Run("local+editor 未配置时放宽", func(t *testing.T) {
		c := &Config{}
		c.Mode = ModeLocal
		c.LocalDS.Launcher = LauncherEditor
		c.Defaults()
		if got := time.Duration(c.Allocator.HeartbeatTimeout); got != 120*time.Second {
			t.Fatalf("HeartbeatTimeout: got %v want 120s", got)
		}
		if got := time.Duration(c.Allocator.ReadyWaitTimeout); got != 300*time.Second {
			t.Fatalf("ReadyWaitTimeout: got %v want 300s", got)
		}
	})

	t.Run("显式配置优先于放宽", func(t *testing.T) {
		c := &Config{}
		c.Mode = ModeLocal
		c.LocalDS.Launcher = LauncherEditor
		c.Allocator.HeartbeatTimeout = config.Duration(7 * time.Second)
		c.Allocator.ReadyWaitTimeout = config.Duration(8 * time.Second)
		c.Defaults()
		if got := time.Duration(c.Allocator.HeartbeatTimeout); got != 7*time.Second {
			t.Fatalf("显式心跳超时被覆盖: got %v want 7s", got)
		}
		if got := time.Duration(c.Allocator.ReadyWaitTimeout); got != 8*time.Second {
			t.Fatalf("显式 ready 等待被覆盖: got %v want 8s", got)
		}
	})

	t.Run("非 local 模式不放宽", func(t *testing.T) {
		c := &Config{}
		c.Mode = ModeAgones
		c.LocalDS.Launcher = LauncherEditor
		c.Defaults()
		if got := time.Duration(c.Allocator.HeartbeatTimeout); got != 15*time.Second {
			t.Fatalf("agones 的心跳超时被放宽了: got %v want 15s", got)
		}
		if got := time.Duration(c.Allocator.ReadyWaitTimeout); got != 10*time.Second {
			t.Fatalf("agones 的 ready 等待被放宽了: got %v want 10s", got)
		}
	})
}

// TestLocalDSLauncherEnvOverride:一键脚本靠环境变量切模式,免改 yaml(与
// PANDORA_DS_EXE / PANDORA_DS_DIR 的注入方式一致,策划机零手改)。
func TestLocalDSLauncherEnvOverride(t *testing.T) {
	dir := t.TempDir()
	proj := filepath.Join(dir, "Pandora.uproject")
	if err := os.WriteFile(proj, []byte("{}"), 0o600); err != nil {
		t.Fatalf("写桩工程失败: %v", err)
	}

	t.Run("PANDORA_DS_LAUNCHER 覆盖 yaml", func(t *testing.T) {
		t.Setenv("PANDORA_DS_LAUNCHER", "editor")
		c := &Config{}
		c.Mode = ModeLocal
		c.LocalDS.Launcher = LauncherPackaged
		c.Defaults()
		if c.LocalDS.Launcher != LauncherEditor {
			t.Fatalf("环境变量未生效: got %q", c.LocalDS.Launcher)
		}
	})

	t.Run("配置的 project_path 缺失时回退到 PANDORA_DS_UPROJECT", func(t *testing.T) {
		t.Setenv("PANDORA_DS_UPROJECT", proj)
		c := &Config{}
		c.Mode = ModeLocal
		c.LocalDS.Launcher = LauncherEditor
		c.LocalDS.ProjectPath = filepath.Join(dir, "missing", "Pandora.uproject")
		c.Defaults()
		if c.LocalDS.ProjectPath != proj {
			t.Fatalf("未回退到环境变量: got %q want %q", c.LocalDS.ProjectPath, proj)
		}
	})

	t.Run("配置的 project_path 存在时不被环境变量覆盖", func(t *testing.T) {
		other := filepath.Join(dir, "Other.uproject")
		if err := os.WriteFile(other, []byte("{}"), 0o600); err != nil {
			t.Fatalf("写桩工程失败: %v", err)
		}
		t.Setenv("PANDORA_DS_UPROJECT", proj)
		c := &Config{}
		c.Mode = ModeLocal
		c.LocalDS.Launcher = LauncherEditor
		c.LocalDS.ProjectPath = other
		c.Defaults()
		if c.LocalDS.ProjectPath != other {
			t.Fatalf("存在的配置路径被覆盖: got %q want %q", c.LocalDS.ProjectPath, other)
		}
	})
}
