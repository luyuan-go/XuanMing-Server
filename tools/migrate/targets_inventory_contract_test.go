// targets_inventory_contract_test.go — 迁移集与目标清单必须一一对上。
//
// 为什么需要这道门:迁移集是**目录**,目标清单是**JSON**,两者没有任何编译期或运行期
// 关联。加了迁移集却忘记加 target,现象是彻底静默的 —— 迁移器只跑清单里点名的库,
// 没被点名的迁移集永远不执行,而 dev 路径(tools/scripts/dev_migrate.ps1 按目录自动
// 枚举)照跑不误,于是本机全绿、生产缺表缺列。
//
// 2026-08-18 一次性抓到三个漏网:pandora_owner(INC-20260818-003 的 hub_source_revision
// 加列所在集,漏了它 = 新 owner 二进制在存量库上启动即 os.Exit(1))、pandora_bag、
// pandora_mission。三者的迁移集都在,targets.example.json 里都没有条目。
//
// 钉的是 targets.example.json:生产真正的目标集合由发布侧 -expected-targets 人工审核
// 提供(deploy/k8s/migrate/job.yaml 里仍是 REPLACE_WITH_REVIEWED_TARGET_INVENTORY 占位),
// 而人是照着这份 example 抄的 —— example 缺一项,审核清单大概率跟着缺一项。
package main

import (
	"encoding/json"
	"io/fs"
	"os"
	"testing"
)

func TestTargetsExampleCoversEveryMigrationSet(t *testing.T) {
	const manifestPath = "targets.example.json"

	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("读取 %s 失败: %v", manifestPath, err)
	}
	var manifest targetManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("%s 不是合法 JSON(或与 targetManifest 结构对不上): %v", manifestPath, err)
	}
	if len(manifest.Targets) == 0 {
		t.Fatalf("%s 里一个 target 都没有", manifestPath)
	}

	// 目标侧自洽:名字唯一、库名合法、migration_set→database 映射合法。
	// 这些规则与 loadTargetManifest 同源,但那条路径会解析 dsn_file 的真实路径,
	// example 里的 *.dsn 并不存在,故这里只复用纯校验部分。
	seen := map[string]bool{}
	covered := map[string]bool{}
	for _, target := range manifest.Targets {
		if !targetNamePattern.MatchString(target.Name) {
			t.Errorf("target name 非法: %q", target.Name)
		}
		if seen[target.Name] {
			t.Errorf("target name 重复: %q", target.Name)
		}
		seen[target.Name] = true
		if !databaseNamePattern.MatchString(target.MigrationSet) {
			t.Errorf("target %s 的 migration_set 非法: %q", target.Name, target.MigrationSet)
		}
		if !validMigrationDatabaseMapping(target.MigrationSet, target.Database) {
			t.Errorf("target %s 的 database=%q 与 migration_set=%q 映射非法",
				target.Name, target.Database, target.MigrationSet)
		}
		if target.DSNFile == "" {
			t.Errorf("target %s 缺 dsn_file", target.Name)
		}
		covered[target.MigrationSet] = true
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("列举内嵌迁移集失败: %v", err)
	}
	var uncovered []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if !covered[entry.Name()] {
			uncovered = append(uncovered, entry.Name())
		}
	}
	if len(uncovered) > 0 {
		t.Fatalf("这些迁移集没有任何 target 会跑到(加迁移集时必须同时加 target): %v", uncovered)
	}

	// 反向:清单不许点名不存在的迁移集 —— 那会让迁移器在运行期才炸。
	onDisk := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() {
			onDisk[entry.Name()] = true
		}
	}
	for _, target := range manifest.Targets {
		if !onDisk[target.MigrationSet] {
			t.Errorf("target %s 点名了不存在的迁移集 %q", target.Name, target.MigrationSet)
		}
	}
}
