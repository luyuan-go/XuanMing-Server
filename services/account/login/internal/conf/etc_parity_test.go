// etc_parity_test.go — login 各档配置的**键集**必须一致(账号 / 角色分离 2026-08-18)。
//
// 为什么钉键集而不是钉值:两档配置的差异应当只有连接串与后端形态(mysql / TiDB),
// 而不该有「某个依赖块只在一档存在」。2026-08-18 的实例正是这样漏的:login-dev.yaml
// 加了 login.player 块(账号名播种成全服显示名),login-dev-tidb.yaml 没跟上,于是
// TiDB 档登录时 addr 为空 → 静默跳过播种 → 角色名全部回落 Player_<id>。
//
// 这类漏配不会让任何单元测试变红(缺的是配置不是代码),也不会让服务启动失败(弱依赖),
// 只在真跑 TiDB 档联调时才暴露 —— 而本机默认联调走的是 mysql 档。
package conf

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var yamlKeyRe = regexp.MustCompile(`^(\s*)([A-Za-z_][A-Za-z0-9_]*):`)

// yamlKeyPaths 提取 yaml 里全部键的点分路径(忽略值、注释、列表项)。
// 只做缩进栈,不引 yaml 库:本测试要的是「块在不在」,不需要完整语义解析,
// 也不值得为此把 gopkg.in/yaml.v3 从 indirect 提成直连依赖。
func yamlKeyPaths(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s 失败: %v", path, err)
	}
	var stack []struct {
		indent int
		key    string
	}
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		m := yamlKeyRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		indent, key := len(m[1]), m[2]
		for len(stack) > 0 && stack[len(stack)-1].indent >= indent {
			stack = stack[:len(stack)-1]
		}
		parts := make([]string, 0, len(stack)+1)
		for _, s := range stack {
			parts = append(parts, s.key)
		}
		parts = append(parts, key)
		out = append(out, strings.Join(parts, "."))
		stack = append(stack, struct {
			indent int
			key    string
		}{indent, key})
	}
	sort.Strings(out)
	return out
}

// TestLoginEtcKeyParity:mysql 档与 TiDB 档必须有完全相同的键集。
//
// 新增依赖块时两档一起改 —— 只改一档就在这里变红,而不是等到联调才发现。
func TestLoginEtcKeyParity(t *testing.T) {
	const (
		mysqlEtc = "../../etc/login-dev.yaml"
		tidbEtc  = "../../etc/login-dev-tidb.yaml"
	)
	mysqlKeys := yamlKeyPaths(t, mysqlEtc)
	tidbKeys := yamlKeyPaths(t, tidbEtc)

	inTiDB := make(map[string]bool, len(tidbKeys))
	for _, k := range tidbKeys {
		inTiDB[k] = true
	}
	inMySQL := make(map[string]bool, len(mysqlKeys))
	for _, k := range mysqlKeys {
		inMySQL[k] = true
	}

	var onlyMySQL, onlyTiDB []string
	for _, k := range mysqlKeys {
		if !inTiDB[k] {
			onlyMySQL = append(onlyMySQL, k)
		}
	}
	for _, k := range tidbKeys {
		if !inMySQL[k] {
			onlyTiDB = append(onlyTiDB, k)
		}
	}
	if len(onlyMySQL) > 0 || len(onlyTiDB) > 0 {
		t.Fatalf("两档配置键集漂移(值可以不同,键必须一致):\n只在 %s: %v\n只在 %s: %v",
			mysqlEtc, onlyMySQL, tidbEtc, onlyTiDB)
	}
}
