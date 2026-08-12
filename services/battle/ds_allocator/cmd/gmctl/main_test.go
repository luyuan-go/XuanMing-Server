package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	gmv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/gm/v1"
)

type gmctlTestServer struct {
	gmv1.UnimplementedGmServiceServer
	metadata chan metadata.MD
}

func (s *gmctlTestServer) SendCommand(
	ctx context.Context,
	_ *gmv1.SendCommandRequest,
) (*gmv1.SendCommandResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.metadata <- md.Copy()
	return &gmv1.SendCommandResponse{
		Code:           commonv1.ErrCode_OK,
		IdempotencyKey: "gmctl-test-command",
	}, nil
}

func TestRunAddItemRequestsExecutionAck(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	testService := &gmctlTestServer{metadata: make(chan metadata.MD, 1)}
	gmv1.RegisterGmServiceServer(server, testService)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	defer func() {
		server.Stop()
		_ = listener.Close()
		<-serveDone
	}()

	if code := runAddItem([]string{
		"--addr", listener.Addr().String(),
		"--match", "42",
		"--player", "1001",
		"--config", "10001",
		"--count", "1",
		"--bag", "0",
	}); code != 0 {
		t.Fatalf("runAddItem exit=%d want 0", code)
	}

	select {
	case md := <-testService.metadata:
		values := md.Get(waitExecutionAckMetadataKey)
		if len(values) != 1 || values[0] != "1" {
			t.Fatalf("%s=%q want [1]", waitExecutionAckMetadataKey, values)
		}
	case <-time.After(time.Second):
		t.Fatal("SendCommand 未到达测试服务")
	}
}
