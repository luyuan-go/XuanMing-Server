package conf

import "testing"

// TestResolveWriterLeaseMode:安全档位配错必须炸,不能静默退化成 off。
// 留空取 enforce —— 与 hub_allocator 同一默认,让仓库只有一套档位语义。
func TestResolveWriterLeaseMode(t *testing.T) {
	for input, want := range map[string]string{
		"":          WriterLeaseEnforce,
		"enforce":   WriterLeaseEnforce,
		"  ENFORCE": WriterLeaseEnforce, // 归一化大小写与空白,避免配错档位却当成合法
		"warmup":    WriterLeaseWarmup,
		"off":       WriterLeaseOff,
	} {
		got, err := AllocatorConf{WriterLeaseMode: input}.ResolveWriterLeaseMode()
		if err != nil || got != want {
			t.Fatalf("ResolveWriterLeaseMode(%q) = %q, %v; want %q, nil", input, got, err, want)
		}
	}
	for _, bad := range []string{"on", "true", "enforced", "disable"} {
		if _, err := (AllocatorConf{WriterLeaseMode: bad}).ResolveWriterLeaseMode(); err == nil {
			t.Fatalf("ResolveWriterLeaseMode(%q) 必须 fail-fast", bad)
		}
	}
}
