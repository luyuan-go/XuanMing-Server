package configtable

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// 本文件是通用加载引擎;批次快照结构(Tables)与逐表注册(specByName)在 tables.gen.go
// (tools/configtable-gen 按表规格生成),表私有校验/域方法在各表手写伴生文件(如 level.go)。

// LoadResult 一次 Load 的结果。
type LoadResult struct {
	Version  uint64
	Reloaded bool     // false = 同版本幂等 no-op(未发生切换)
	Warnings []string // 非致命项(未知表跳过 / 目录脏文件),调用方记告警日志
}

// Store 配置表持有者:Load 全批构建 + 校验成功才原子切换,失败保留旧批次。
// 读路径无锁(atomic.Pointer);Load 由互斥锁单飞,并发 reload 串行化。
type Store struct {
	mu         sync.Mutex
	cur        atomic.Pointer[Tables]
	validators []func(*Tables) error
}

// NewStore 创建空 Store;服务启动必须先 Load 成功再对外服务(fail-closed)。
func NewStore() *Store { return &Store{} }

// AddValidator 注册批次级语义校验(服务特有约束,如 matchmaker 的"默认 map 必须是
// 战斗关卡")。Load 在全部表构建 + 跨表校验通过后、指针切换前运行;任一失败整批不切换
// (审计 P1:启动门禁必须在每次热 reload 同样生效,否则坏批次热更后新请求全部失败)。
// 只允许在首次 Load 前注册(启动期单线程接线),运行期不得再调用。
func (s *Store) AddValidator(v func(*Tables) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.validators = append(s.validators, v)
}

// Tables 当前生效批次;从未加载成功过返回 nil。
func (s *Store) Tables() *Tables { return s.cur.Load() }

// Load 从 activeDir 加载一批配置表并在全部成功后原子切换。
//
// expectVersion 非 0 时要求 manifest.version 恰等于它(发布脚本确认「刚发布那批」已生效)。
// 版本语义(config-table-hotreload.md §5/§6):
//   - version == 当前生效 → 幂等 no-op(Reloaded=false, err=nil);
//   - version <  当前生效 → 拒绝(防回退 / 重放),回滚走重新发布更高版本;
//   - 任一表读取 / checksum / 解析 / 行数 / 表内校验失败 → 整批不切换,返回 error。
func (s *Store) Load(activeDir string, expectVersion uint64) (*LoadResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, err := ReadManifest(activeDir)
	if err != nil {
		return nil, err
	}
	if expectVersion != 0 && m.Version != expectVersion {
		return nil, fmt.Errorf("active 版本 %d 与期望 %d 不符,拒绝加载", m.Version, expectVersion)
	}
	if cur := s.cur.Load(); cur != nil {
		if m.Version == cur.Version {
			return &LoadResult{Version: cur.Version, Reloaded: false}, nil
		}
		if m.Version < cur.Version {
			return nil, fmt.Errorf("active 版本 %d 低于当前生效 %d,拒绝回退", m.Version, cur.Version)
		}
	}

	next := &Tables{Version: m.Version, SourceRev: m.SourceRev}
	var warnings []string
	loaded := make(map[string]bool, len(m.Tables))
	for _, mt := range m.Tables {
		spec, known := specByName[mt.Name]
		if !known {
			// 前向兼容:清单里出现本进程还不认识的新表 → 跳过并告警,
			// 滚动更新窗口内旧副本不因新表发布而热更失败(不变量 §21)。
			warnings = append(warnings, fmt.Sprintf("跳过未注册表 %q(本进程不认识,可能是新表先于新版本发布)", mt.Name))
			continue
		}
		if mt.Proto != "" && mt.Proto != spec.protoName {
			return nil, fmt.Errorf("表 %q 声明 proto %q 与注册的 %q 不符", mt.Name, mt.Proto, spec.protoName)
		}
		raw, err := os.ReadFile(filepath.Join(activeDir, mt.File))
		if err != nil {
			return nil, fmt.Errorf("表 %q 读文件失败: %w", mt.Name, err)
		}
		if err := VerifyChecksum(raw, mt.Checksum); err != nil {
			return nil, fmt.Errorf("表 %q %w", mt.Name, err)
		}
		if err := spec.build(raw, mt, next); err != nil {
			return nil, err
		}
		loaded[mt.Name] = true
	}
	for name := range specByName {
		if !loaded[name] {
			return nil, fmt.Errorf("manifest 缺少本进程必需的表 %q,整批拒绝", name)
		}
	}
	// 跨表引用完整性((excel_fk),tables.gen.go 生成):全过才允许切换。
	if err := validateCrossTables(next); err != nil {
		return nil, err
	}
	// 服务注册的批次级语义校验(启动与热 reload 同一门禁):失败整批不切换,保留旧批次。
	for _, v := range s.validators {
		if err := v(next); err != nil {
			return nil, fmt.Errorf("批次 v%d 语义校验失败(保留旧批次): %w", m.Version, err)
		}
	}
	warnings = append(warnings, strayFileWarnings(activeDir, m)...)

	s.cur.Store(next)
	return &LoadResult{Version: m.Version, Reloaded: true, Warnings: warnings}, nil
}

// tableSpec 表注册项:清单表名 → 解析构建函数(实例见 tables.gen.go)。
type tableSpec struct {
	protoName string // 容器 message 全名,与 manifest.proto 比对防接错文件
	build     func(raw []byte, mt ManifestTable, dst *Tables) error
}

// unmarshalTable 运行时读表:protojson + DiscardUnknown。
// 严格校验(未知字段报错)只放生成阶段;运行时容忍新增字段,
// 滚动窗口内旧进程能读新 JSON(hotreload doc §8.3、不变量 §21)。
func unmarshalTable(raw []byte, msg proto.Message) error {
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, msg)
}

// strayFileWarnings active 目录里 manifest 之外的 *.json 视为脏数据,告警不拒载
// (hotreload doc §5:服务端只加载 manifest 列出的表)。
func strayFileWarnings(dir string, m *Manifest) []string {
	listed := make(map[string]bool, len(m.Tables)+1)
	listed[ManifestFileName] = true
	for _, t := range m.Tables {
		listed[t.File] = true
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return []string{fmt.Sprintf("脏文件检查失败: %v", err)}
	}
	var warns []string
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		if !listed[e.Name()] {
			warns = append(warns, fmt.Sprintf("active 目录存在 manifest 未列出的文件 %q(脏数据)", e.Name()))
		}
	}
	return warns
}

// RegisteredTable 本二进制注册的一张配置表(注册名 + proto 全名)。
type RegisteredTable struct {
	Name  string
	Proto string
}

// RegisteredTables 列出本进程注册的全部配置表,按注册名升序。
//
// 用途:Load 要求 manifest **覆盖全部**注册表,缺一张就整批拒绝(见上文 "缺少本进程必需的表")。
// 这条约束是单向的——清单里多出本进程不认识的表只告警跳过,少一张则直接拒绝。也就是说
// **二进制先于配置发布是不兼容的**,必须先让新批次(dist / ConfigMap)就位再滚二进制。
// 把注册表暴露出来,是为了让"这个二进制到底需要哪些表"可被机械核对,而不是靠人去数
// proto 目录:发布前核对 ConfigMap 是否齐备、测试夹具补齐批次,都用它。
func RegisteredTables() []RegisteredTable {
	out := make([]RegisteredTable, 0, len(specByName))
	for name, spec := range specByName {
		out = append(out, RegisteredTable{Name: name, Proto: spec.protoName})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
