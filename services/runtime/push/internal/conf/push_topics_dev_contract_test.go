// push_topics_dev_contract_test.go — push-dev.yaml 显式 topics 清单与 kafkax.PushTopics
// 的机械对齐门禁(2026-08-11)。
//
// 背景:push.topics 留空才走 kafkax.PushTopics 权威默认集合;push-dev.yaml **显式列了**
// 清单,于是它是覆盖而非追加 —— 新业务上线时只在 topics.go 加常量、忘了改 dev yaml,
// 该事件在本地栈里就完全不被消费,表现是"出箱一直堆积、客户端收不到推送",而所有
// 单测和构建都是绿的。历史上 push-prod.yaml.example 正因此漏过 chat.guild / chat.group /
// guild.event / presence.update 四条,才改成不列;2026-08-11 mission.update 又在 dev 侧
// 重演了一次。本测试把这条对齐钉死:两边漂移即失败,并指出到底差了哪几条。
package conf

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/luyuancpp/pandora/pkg/kafkax"
)

func TestPushDevYamlTopicsMatchKafkaxPushTopics(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位测试文件")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "etc", "push-dev.yaml"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}

	got := parseDevTopics(t, string(raw))
	if len(got) == 0 {
		// 留空也是合法形态(等价于走默认集合),但那样本测试无对象可比,提示改判据。
		t.Fatal("push-dev.yaml 未显式列出 topics;若已改为留空(走 kafkax.PushTopics 默认),请删掉本测试")
	}

	want := make(map[string]bool, len(kafkax.PushTopics))
	for _, topic := range kafkax.PushTopics {
		want[topic] = true
	}
	have := make(map[string]bool, len(got))
	for _, topic := range got {
		if have[topic] {
			t.Fatalf("push-dev.yaml topics 重复: %s", topic)
		}
		have[topic] = true
	}

	var missing, extra []string
	for _, topic := range kafkax.PushTopics {
		if !have[topic] {
			missing = append(missing, topic)
		}
	}
	for _, topic := range got {
		if !want[topic] {
			extra = append(extra, topic)
		}
	}
	if len(missing) > 0 {
		t.Errorf("push-dev.yaml 漏订阅 %v:这些事件在本地栈完全不消费(出箱堆积且无任何报错)。"+
			"新增 topic 必须同时改 pkg/kafkax/topics.go 与本 yaml", missing)
	}
	if len(extra) > 0 {
		t.Errorf("push-dev.yaml 多出 %v:不在 kafkax.PushTopics 里,消费会一直空转", extra)
	}
}

// parseDevTopics 从 yaml 文本里抽 push.topics 列表(只认 `    - "xxx"` 形态,
// 足够覆盖本文件的固定写法;引入 yaml 解析库只为一个清单不值当)。
func parseDevTopics(t *testing.T, raw string) []string {
	t.Helper()
	lines := strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n")
	var (
		out      []string
		inTopics bool
	)
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "topics:" {
			if inTopics {
				t.Fatal("push-dev.yaml 出现多个 topics: 块")
			}
			inTopics = true
			continue
		}
		if !inTopics {
			continue
		}
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if !strings.HasPrefix(trimmed, "- ") {
			break // 列表结束
		}
		out = append(out, strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "- ")), `"'`))
	}
	return out
}
