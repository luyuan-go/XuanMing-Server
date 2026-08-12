package service

import (
	"strings"
	"testing"

	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// TestLoginResponsePlayerNoExpandCompatibility 验证滚动期双写同时兼容两类已发布调用方：
// 旧 protobuf 二进制继续按 wire #13 读值；4e 期间的 JSON 调用方继续读 playerNo。
func TestLoginResponsePlayerNoExpandCompatibility(t *testing.T) {
	const want = uint64(100001)
	raw, err := proto.Marshal(&loginv1.LoginResponse{PlayerNo: want, RegisterNo: want})
	if err != nil {
		t.Fatalf("marshal LoginResponse: %v", err)
	}

	var legacyRegisterNo uint64
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			t.Fatalf("consume tag: %v", protowire.ParseError(n))
		}
		raw = raw[n:]
		if typ != protowire.VarintType {
			n = protowire.ConsumeFieldValue(num, typ, raw)
			if n < 0 {
				t.Fatalf("consume field %d: %v", num, protowire.ParseError(n))
			}
			raw = raw[n:]
			continue
		}
		value, valueLen := protowire.ConsumeVarint(raw)
		if valueLen < 0 {
			t.Fatalf("consume varint field %d: %v", num, protowire.ParseError(valueLen))
		}
		raw = raw[valueLen:]
		if num == 13 { // 旧 register_no 与 4e player_no 二进制都编译为 wire #13。
			legacyRegisterNo = value
		}
	}
	if legacyRegisterNo != want {
		t.Fatalf("legacy wire #13 = %d, want %d", legacyRegisterNo, want)
	}

	jsonBytes, err := protojson.Marshal(&loginv1.LoginResponse{PlayerNo: want, RegisterNo: want})
	if err != nil {
		t.Fatalf("marshal LoginResponse JSON: %v", err)
	}
	jsonText := string(jsonBytes)
	if !strings.Contains(jsonText, `"registerNo":"100001"`) {
		t.Fatalf("legacy JSON registerNo missing: %s", jsonText)
	}
	if !strings.Contains(jsonText, `"playerNo":"100001"`) {
		t.Fatalf("player JSON playerNo missing: %s", jsonText)
	}
}
