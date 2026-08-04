package conf

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLocalHubLauncherDefaults 锁住「加 editor 模式不影响原有大厅链路」的不变量:
// 老配置(不写 launcher)必须归一化成 packaged。
func TestLocalHubLauncherDefaults(t *testing.T) {
	os.Unsetenv("PANDORA_DS_LAUNCHER")
	os.Unsetenv("PANDORA_DS_UPROJECT")

	for _, in := range []string{"", "listen", "packaged", "  PACKAGED  "} {
		c := &Config{}
		c.Mode = ModeLocal
		c.LocalHub.Launcher = in
		c.Defaults()
		if c.LocalHub.Launcher != LauncherPackaged {
			t.Fatalf("launcher(%q): got %q want %q", in, c.LocalHub.Launcher, LauncherPackaged)
		}
	}

	c := &Config{}
	c.Mode = ModeLocal
	c.LocalHub.Launcher = " Editor "
	c.Defaults()
	if c.LocalHub.Launcher != LauncherEditor {
		t.Fatalf("launcher: got %q want %q", c.LocalHub.Launcher, LauncherEditor)
	}
}

// TestLocalHubLauncherEnvOverride:一键脚本靠环境变量切模式,免改 yaml
// (同一对环境变量同时作用于大厅 DS 与战斗 DS,策划机零手改)。
func TestLocalHubLauncherEnvOverride(t *testing.T) {
	dir := t.TempDir()
	proj := filepath.Join(dir, "Pandora.uproject")
	if err := os.WriteFile(proj, []byte("{}"), 0o600); err != nil {
		t.Fatalf("写桩工程失败: %v", err)
	}

	t.Run("PANDORA_DS_LAUNCHER 覆盖 yaml", func(t *testing.T) {
		t.Setenv("PANDORA_DS_LAUNCHER", "editor")
		c := &Config{}
		c.Mode = ModeLocal
		c.LocalHub.Launcher = LauncherPackaged
		c.Defaults()
		if c.LocalHub.Launcher != LauncherEditor {
			t.Fatalf("环境变量未生效: got %q", c.LocalHub.Launcher)
		}
	})

	t.Run("配置的 project_path 缺失时回退到 PANDORA_DS_UPROJECT", func(t *testing.T) {
		t.Setenv("PANDORA_DS_UPROJECT", proj)
		c := &Config{}
		c.Mode = ModeLocal
		c.LocalHub.Launcher = LauncherEditor
		c.LocalHub.ProjectPath = filepath.Join(dir, "missing", "Pandora.uproject")
		c.Defaults()
		if c.LocalHub.ProjectPath != proj {
			t.Fatalf("未回退到环境变量: got %q want %q", c.LocalHub.ProjectPath, proj)
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
		c.LocalHub.Launcher = LauncherEditor
		c.LocalHub.ProjectPath = other
		c.Defaults()
		if c.LocalHub.ProjectPath != other {
			t.Fatalf("存在的配置路径被覆盖: got %q want %q", c.LocalHub.ProjectPath, other)
		}
	})
}
