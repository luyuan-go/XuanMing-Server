package service

import (
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
)

func TestProgressResponsePreservesDurableTerminalAck(t *testing.T) {
	terminal := progressResponse(7, errcode.New(errcode.ErrInvalidArg, "persisted action failure"))
	if terminal.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG || terminal.GetAckedSeq() != 7 {
		t.Fatalf("terminal response=%+v", terminal)
	}
	retryable := progressResponse(0, errcode.New(errcode.ErrUnavailable, "inventory unavailable"))
	if retryable.GetCode() != commonv1.ErrCode_ERR_UNAVAILABLE || retryable.GetAckedSeq() != 0 {
		t.Fatalf("retryable response=%+v", retryable)
	}
	success := progressResponse(8, nil)
	if success.GetCode() != commonv1.ErrCode_OK || success.GetAckedSeq() != 8 {
		t.Fatalf("success response=%+v", success)
	}
}
