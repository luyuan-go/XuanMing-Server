package service

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/auth"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	"github.com/luyuancpp/pandora/services/account/login/internal/biz"
)

type playerNoAccountRepo struct{ playerNo uint64 }

func (r *playerNoAccountRepo) FindByAccount(context.Context, string) (uint64, string, error) {
	return 42, "", nil
}
func (r *playerNoAccountRepo) CreateAccount(context.Context, uint64, string, string) error {
	return nil
}
func (r *playerNoAccountRepo) CheckBanned(context.Context, uint64, string) (bool, error) {
	return false, nil
}
func (r *playerNoAccountRepo) TouchDevice(context.Context, uint64, string) error { return nil }
func (r *playerNoAccountRepo) GetPlayerNo(context.Context, uint64) (uint64, error) {
	return r.playerNo, nil
}

func TestLoginResponseCarriesPlayerNo(t *testing.T) {
	const want = uint64(100001)
	cfg := auth.Config{Secret: []byte("pandora-player-no-service-test-32!!")}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	uc := biz.NewLoginUsecase(&playerNoAccountRepo{playerNo: want}, nil, nil, nil, nil, nil,
		"127.0.0.1:7777", "cn", signer, verifier, nil, true, false, nil, false)
	svc := NewLoginService(uc, nil)

	res, rpcErr := svc.Login(context.Background(), &loginv1.LoginRequest{
		Account: "acc", PasswordHash: "ignored-in-dev-skip", DeviceId: "dev-1",
	})
	if rpcErr != nil {
		t.Fatalf("Login transport error: %v", rpcErr)
	}
	if res.GetCode() != commonv1.ErrCode_OK {
		t.Fatalf("Login code = %v, want OK", res.GetCode())
	}
	if res.GetPlayerNo() != want {
		t.Fatalf("LoginResponse.player_no = %d, want %d", res.GetPlayerNo(), want)
	}
}
