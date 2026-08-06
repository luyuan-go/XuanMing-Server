// Command gmctl 是 GM / 运维指令下发 CLI —— pandora.gm.v1.GmService.SendCommand 的生产者。
//
// 补上闭环的「下发」这一端:ds_allocator 内已注册 GmService(server)+ 战斗 DS 已轮询消费
// (PollCommands/AckCommand),但此前无任何进程调用 SendCommand。本 CLI 让运维能真正把
// 「给对局内某玩家发道具」指令下发到目标战斗 DS,构成可用闭环(CLAUDE.md §14 接线完整性)。
//
// 送达语义:at-most-once(尽力而为,见 proto pandora/gm/v1/gm.proto)。GM 调试指令可容忍
// 偶发丢失。⚠️ 本命令**非幂等**:每次执行服务端都现生成新 idempotency_key,入队一条
// 全新指令——若上一次其实已生效而盲目重跑,会**重复发放**。idempotency_key 只防「同一条
// 已入队指令」被 DS 重复执行,不防运维重复下发。重跑前请先确认上一次是否真未生效。
//
// 用法:
//
//	gmctl additem --addr 127.0.0.1:20020 --match <matchID> --player <playerID> \
//	              --config <configID> [--count 1] [--bag 0]
//
// 地址默认取环境变量 PANDORA_DS_ALLOCATOR_ADDR,再回退 127.0.0.1:20020(与 UE DS 侧一致)。
// 内部接口,直连 ds_allocator gRPC 端口,不经 Envoy。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	gmv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/gm/v1"
)

const (
	defaultAddr = "127.0.0.1:20020"
	dialTimeout = 10 * time.Second
	// maxUint32Config 道具 config_id 上限(proto uint32),--config 是 uint 防截断越界转。
	maxUint32Config = uint(^uint32(0))
	// maxInt32Count 发放数量上限(proto int32),防 --count 转 int32 溢出。
	maxInt32Count = int32(^uint32(0) >> 1)
	// maxBagType 背包类型上限(0=人物背包 1=仓库 2=装备栏 3=临时格)。
	maxBagType = 3
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "additem":
		os.Exit(runAddItem(os.Args[2:]))
	case "-h", "--help", "help":
		usage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "未知子命令:%q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `gmctl —— GM 指令下发 CLI(GmService.SendCommand 生产者)

用法:
  gmctl additem --match <matchID> --player <playerID> --config <configID> [--count 1] [--bag 0] [--addr host:port]

子命令:
  additem   给对局内指定玩家发道具(远程触发 DS 本地命令 My.DS.GM.AddItem)

地址解析优先级:--addr > 环境变量 PANDORA_DS_ALLOCATOR_ADDR > 127.0.0.1:20020
`)
}

func runAddItem(args []string) int {
	fs := flag.NewFlagSet("additem", flag.ExitOnError)
	addr := fs.String("addr", "", "ds_allocator gRPC 地址(默认 env PANDORA_DS_ALLOCATOR_ADDR / 127.0.0.1:20020)")
	match := fs.Uint64("match", 0, "目标对局 match_id(必填,>0)")
	player := fs.Uint64("player", 0, "目标玩家 player_id(必填,Snowflake uint64)")
	config := fs.Uint("config", 0, "道具配置 id(必填,>0)")
	count := fs.Int("count", 1, "发放数量(>0)")
	bag := fs.Int("bag", 0, "背包类型(0=人物背包 1=仓库 2=装备栏 3=临时格)")
	_ = fs.Parse(args)

	if *match == 0 || *player == 0 || *config == 0 || *config > maxUint32Config ||
		*count <= 0 || *count > int(maxInt32Count) || *bag < 0 || *bag > maxBagType {
		fmt.Fprintf(os.Stderr,
			"参数非法:--match/--player/--config 必须 > 0;--config ≤ %d;--count 必须在 1..%d;--bag 必须在 0..%d\n",
			maxUint32Config, maxInt32Count, maxBagType)
		fs.Usage()
		return 2
	}

	endpoint := resolveAddr(*addr)

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "连接 ds_allocator 失败(%s):%v\n", endpoint, err)
		return 1
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	resp, err := gmv1.NewGmServiceClient(conn).SendCommand(ctx, &gmv1.SendCommandRequest{
		MatchId: *match,
		Payload: &gmv1.SendCommandRequest_AddItem{AddItem: &gmv1.AddItemCommand{
			PlayerId: *player,
			ConfigId: uint32(*config),
			Count:    int32(*count),
			BagType:  int32(*bag),
		}},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "SendCommand 失败:%v\n", err)
		return 1
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		fmt.Fprintf(os.Stderr, "SendCommand 被拒:code=%s\n", resp.GetCode())
		return 1
	}

	fmt.Printf("已入队:match=%d player=%d config=%d count=%d bag=%d idempotency_key=%s\n",
		*match, *player, *config, *count, *bag, resp.GetIdempotencyKey())
	return 0
}

// resolveAddr 按 --addr > env PANDORA_DS_ALLOCATOR_ADDR > 默认值 解析目标地址。
func resolveAddr(flagAddr string) string {
	if flagAddr != "" {
		return flagAddr
	}
	if env := os.Getenv("PANDORA_DS_ALLOCATOR_ADDR"); env != "" {
		return env
	}
	return defaultAddr
}
