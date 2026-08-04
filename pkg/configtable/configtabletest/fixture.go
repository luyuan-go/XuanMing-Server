// Package configtabletest 提供配置表批次的测试夹具工具。
//
// 存在理由:configtable.Store.Load 要求 manifest **覆盖本进程注册的全部表**,缺一张整批拒绝。
// 于是每个服务的测试都得把与自己无关的表也写进批次,否则加张新配置表就会连带压垮一堆
// 无关用例(2026-08-04 一次性登记 19 张客户端表时,三个包的夹具同时挂掉)。
// 这里把"补齐其余表"这件事收成一处,各测试只需构造自己真正断言的那几张表。
package configtabletest

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/luyuancpp/pandora/pkg/configtable"
)

// FillMissingTables 为 present 之外的每一张已注册表,在 dir 写出一份空表产物,
// 并返回它们对应的 manifest 条目(与生成器同一 JSON 口径)。
//
// 空表(rows 为空)对加载引擎是合法批次;调用方自己那几张有行的表若不引用这些空表,
// 跨表引用完整性校验照常通过。若调用方的表**确实**引用了某张表的行,就必须自己
// 构造那张表而不是依赖本函数补空表。
//
// 返回条目按注册名升序(RegisteredTables 已排序),保证夹具产物可复现。
func FillMissingTables(t *testing.T, dir string, present []string) []map[string]any {
	t.Helper()
	have := make(map[string]bool, len(present))
	for _, name := range present {
		have[name] = true
	}
	var out []map[string]any
	for _, reg := range configtable.RegisteredTables() {
		if have[reg.Name] {
			continue
		}
		mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(reg.Proto))
		if err != nil {
			t.Fatalf("夹具补表 %q: 找不到 proto 类型 %q: %v", reg.Name, reg.Proto, err)
		}
		raw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.
			Marshal(mt.New().Interface())
		if err != nil {
			t.Fatalf("夹具补表 %q: 序列化空表失败: %v", reg.Name, err)
		}
		file := reg.Name + ".json"
		if err := os.WriteFile(filepath.Join(dir, file), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		out = append(out, map[string]any{
			"name": reg.Name, "file": file, "proto": reg.Proto,
			"checksum": "sha256:" + hex.EncodeToString(sum[:]), "rows": 0,
		})
	}
	return out
}
