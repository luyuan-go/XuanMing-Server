package conf

import "testing"

// R10 复审 P0-5:写者继任租约档位是安全开关,必须"留空=保持现网 enforce",
// 且非法值 fail-fast(不允许静默退化成不设防的 off/warmup)。
func TestResolveWriterLeaseMode(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "", want: WriterLeaseEnforce},
		{in: WriterLeaseEnforce, want: WriterLeaseEnforce},
		{in: WriterLeaseWarmup, want: WriterLeaseWarmup},
		{in: WriterLeaseOff, want: WriterLeaseOff},
		{in: "Enforce", wantErr: true}, // 大小写不宽容:避免"看起来配对了"的静默变形
		{in: "true", wantErr: true},
		{in: "on", wantErr: true},
	}
	for _, c := range cases {
		got, err := HubConf{WriterLeaseMode: c.in}.ResolveWriterLeaseMode()
		if c.wantErr {
			if err == nil {
				t.Fatalf("writer_lease_mode %q must fail fast, got %q", c.in, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Fatalf("writer_lease_mode %q → (%q, %v), want %q", c.in, got, err, c.want)
		}
	}
}
