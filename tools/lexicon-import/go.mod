module github.com/luyuancpp/pandora/tools/lexicon-import

go 1.26.5

// 敏感词库导入器:configtable/lexicon/source(GitHub 上游快照)→ 合并去重归一化 →
// configtable/lexicon/dist/{lexicon.json,manifest.json}。
// 归一化复用 pkg/namecheck 的 Normalize/Fold —— 与运行时同一份函数,口径不可能漂移。
// 除 pkg 外零依赖(x/text 由 pkg 间接带入,用于 NFKC)。

require github.com/luyuancpp/pandora/pkg v0.0.0-00010101000000-000000000000

require golang.org/x/text v0.37.0 // indirect

replace github.com/luyuancpp/pandora/pkg => ../../pkg
