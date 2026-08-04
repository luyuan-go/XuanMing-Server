// cache_test.go —— guild 读缓存纯函数逻辑测试(不依赖 Redis)。
//
// 覆盖滚动升级缓存投毒防护的核心判定(writerHasAllReaderFields)、
// GuildRow ↔ guildv1.Guild 快照转换、key 格式与 member 反查编解码,
// 避免 §9 不变量 16/17 的位图超集判定回归。
package data

import (
	"encoding/binary"
	"testing"
)

func TestWriterHasAllReaderFields(t *testing.T) {
	cases := []struct {
		name   string
		writer []byte
		reader []byte
		want   bool
	}{
		{"equal", []byte{0b0111_1110}, []byte{0b0111_1110}, true},
		{"writer_superset", []byte{0b0111_1111}, []byte{0b0111_1110}, true},
		{"writer_missing_one", []byte{0b0111_1100}, []byte{0b0111_1110}, false}, // reader 有 bit1,writer 缺
		{"writer_shorter_but_covers", []byte{0b0000_0110}, []byte{0b0000_0110, 0}, true},
		{"writer_shorter_missing", []byte{0b0000_0110}, []byte{0b0000_0110, 0b1}, false}, // reader 高字节有位,writer 无
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := writerHasAllReaderFields(c.writer, c.reader); got != c.want {
				t.Fatalf("writerHasAllReaderFields(%v,%v)=%v want %v", c.writer, c.reader, got, c.want)
			}
		})
	}
}

func TestGuildCacheSchemaMaskCoversAllFields(t *testing.T) {
	// 本副本对自身字段集必是超集(自反),否则永远 miss。
	if !writerHasAllReaderFields(guildCacheSchemaMask, guildCacheSchemaMask) {
		t.Fatal("schema mask not superset of itself")
	}
	// guildv1.Guild 现有 6 个字段(1..6),位图至少覆盖到 bit6。
	if len(guildCacheSchemaMask) == 0 || guildCacheSchemaMask[0]&(1<<6) == 0 {
		t.Fatalf("schema mask missing field 6: %v", guildCacheSchemaMask)
	}
}

func TestGuildRowProtoRoundTrip(t *testing.T) {
	in := &GuildRow{GuildID: 42, Name: "阿瓦隆", LeaderID: 7, MemberCount: 3, MaxMembers: 100, CreatedMs: 1_700_000_000_000}
	out := guildProtoToRow(guildRowToProto(in))
	if *out != *in {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", out, in)
	}
}

func TestGuildCacheKeysHashtag(t *testing.T) {
	if got := guildInfoKey(42); got != "pandora:guild:info:{42}" {
		t.Fatalf("guildInfoKey=%q", got)
	}
	if got := guildMemberKey(7); got != "pandora:guild:member:{7}" {
		t.Fatalf("guildMemberKey=%q", got)
	}
}

func TestMemberValueLayout(t *testing.T) {
	buf := make([]byte, memberValueLen)
	copy(buf[:cacheMagicLen], guildMemberMagic[:])
	binary.BigEndian.PutUint64(buf[cacheMagicLen:], 12345)
	if len(buf) != cacheMagicLen+8 {
		t.Fatalf("memberValueLen=%d want %d", memberValueLen, cacheMagicLen+8)
	}
	if binary.BigEndian.Uint64(buf[cacheMagicLen:]) != 12345 {
		t.Fatal("member value guild_id decode mismatch")
	}
}
