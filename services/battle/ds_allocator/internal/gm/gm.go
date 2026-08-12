// Package gm 是 GM / 运维指令下发服务(pandora.gm.v1.GmService),与 ds_allocator 同进程。
//
// 送达模型(队列 + 轮询,见 proto pandora/gm/v1/gm.proto):
//   - SendCommand:运维 / GM 工具下发指令 → 按 match_id 入 Redis 队列(LPUSH);
//     gmctl 显式要求等待时,在有界 deadline 内等 DS 执行 Ack。
//   - PollCommands:战斗 DS 复用心跳节奏轮询拉取自己这局的指令(RPOP,取即出队,FIFO)。
//   - AckCommand:DS 回报执行结果(审计 + 唤醒对应的等待型 SendCommand,不影响队列)。
//
// 为什么宿主在 ds_allocator:它已持有 match_id→战斗 DS 的注册表,DS 也已与之心跳直连,
// GmService 复用同一 gRPC 端口,内部接口不经 Envoy 暴露给玩家客户端。
//
// Redis key(hashtag 锁同一 slot,Redis Cluster 兼容):
//
//	pandora:gm:queue:{<match_id>} → LIST，每个元素是一条 GmCommand proto bytes。
//	pandora:gm:ack:{<match_id>}:<idempotency_key>:{result,signal} → 有 TTL 的执行结果/阻塞唤醒。
//
// 送达语义:RPOP 取即出队,属 **at-most-once(尽力而为)**——DS 拉取后若在执行前宕机
// 该指令会丢失,不自动重投。GM 调试指令可容忍偶发丢失,失败由运维重发。指令带
// idempotency_key(每次 SendCommand 现生成的 UUID,非业务 ID),DS 按此去重——只防「同一条
// 已入队指令」被重复投递 / 重复拉取,**不防运维重复下发**(每次 SendCommand 都是新 key = 新
// 指令 = 再发一次道具)。防重复发放靠运维不重复下发,本服务不代劳。
package gm

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/middleware"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
	gmv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/gm/v1"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

const (
	// defaultPollMax 单次 PollCommands 默认拉取条数(req.max<=0 时用)。
	defaultPollMax = 16
	// maxPollMax 单次 PollCommands 拉取条数上限(夹取 req.max,防一次拉爆)。
	maxPollMax = 64
	// maxQueueLen 单局队列最大堆积条数(超过丢最旧,防 DS 长时间不轮询时无限堆积)。
	maxQueueLen = 256
	// queueTTL 队列 key 存活时长;每次入队刷新,对局结束后无人续则自动清理,防僵尸队列。
	queueTTL = 30 * time.Minute
	// executionAckWaitTimeout 是远程 GM 命令等待 DS 执行 Ack 的服务端上界。
	// Battle DS 复用 5s 心跳轮询,取 15s 覆盖三个轮询周期;调用方 deadline 更短时以调用方为准。
	executionAckWaitTimeout = 15 * time.Second
	// executionAckTTL 覆盖等待窗口及 Ack 回包丢失后的有界重试,且避免临时结果无限增长。
	executionAckTTL = 10 * time.Minute
	// executionAckBlockSlice 是单次 Redis BLPOP 的最大阻塞片段。公共 Redis client
	// 为保持旧服务语义不启用 ContextTimeoutEnabled,因此用 1s 阻塞片段有界感知
	// 调用方 cancel;每个片段仍由 Redis 事件唤醒,不做状态忙轮询。
	executionAckBlockSlice = time.Second
	// maxBagType 背包类型上限(0=人物背包 1=仓库 2=装备栏 3=临时格),越界拒掉。
	maxBagType = 3
	// maxAckMessageBytes 限制 DS 自由文本失败原因的 Redis/日志体积。
	maxAckMessageBytes = 512
	// maxAckIdempotencyKeyBytes 覆盖服务端生成的 36 字节 UUID,并防止异常长 Redis key。
	maxAckIdempotencyKeyBytes = 64

	waitExecutionAckMetadataKey    = "x-pandora-gm-wait-execution-ack"
	waitExecutionAckMetadataValue  = "1"
	executionAckMessageMetadataKey = "x-pandora-gm-execution-message-bin"
	executionAckPendingValue       = "pending"
)

type executionAck struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// storeExecutionAckScript 以首次 Ack 为准原子地将 pending 转成终态并唤醒等待者。
// result/signal 与指令队列共用 match_id hashtag,可在 Redis Cluster 同 slot 执行。
// 没有 pending 表示旧的非等待调用或过期 Ack,保持只审计的旧行为。
var storeExecutionAckScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  return false
end
if current == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
  redis.call('LPUSH', KEYS[2], ARGV[2])
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
  return ARGV[2]
end
return current
`)

// BattleLivenessChecker 查某对局是否存在活跃战斗镜像(SendCommand 前置校验目标对局有效)。
// *data.RedisBattleRepo 天然满足(GetBattle);为 nil 时跳过校验(保留旧行为,便于单测)。
type BattleLivenessChecker interface {
	GetBattle(ctx context.Context, matchID uint64) (*dsv1.BattleStorageRecord, bool, error)
}

// queueKey 返回某对局的 GM 指令队列 key(hashtag 锁 slot,Cluster 兼容)。
func queueKey(matchID uint64) string {
	return "pandora:gm:queue:{" + strconv.FormatUint(matchID, 10) + "}"
}

func executionAckKeyPrefix(matchID uint64, idempotencyKey string) string {
	return "pandora:gm:ack:{" + strconv.FormatUint(matchID, 10) + "}:" + idempotencyKey
}

func executionAckResultKey(matchID uint64, idempotencyKey string) string {
	return executionAckKeyPrefix(matchID, idempotencyKey) + ":result"
}

func executionAckSignalKey(matchID uint64, idempotencyKey string) string {
	return executionAckKeyPrefix(matchID, idempotencyKey) + ":signal"
}

// Service 实现 gmv1.GmServiceServer。
type Service struct {
	gmv1.UnimplementedGmServiceServer
	rdb           redis.UniversalClient
	helper        *log.Helper
	battleChecker BattleLivenessChecker       // 可选:SendCommand 前置校验目标对局活跃;nil 则不校验
	dsGuard       *middleware.DSCallbackGuard // 可选:PollCommands/AckCommand 的 DS 回调令牌守卫(审核 P1 #1);nil 等价 off
	battleAuth    data.BattleAuthRepo         // Model B Redis 唯一授权权威
	modelB        bool
}

// NewService 构造 GmService。
func NewService(rdb redis.UniversalClient, logger log.Logger) *Service {
	return &Service{rdb: rdb, helper: log.NewHelper(logger)}
}

// SetBattleChecker 注入对局活跃性校验器(可选依赖,同 uc.SetLifecyclePusher 风格)。
// 注入后 SendCommand 会先校验 match_id 对应的战斗镜像是否存在;不注则跳过(保留旧行为)。
func (s *Service) SetBattleChecker(c BattleLivenessChecker) { s.battleChecker = c }

// SetDSCallbackGuard 注入 DS 回调令牌守卫(可选依赖,main 在 ds_auth 已配时调用)。
// 只管 DS 侧的 PollCommands/AckCommand;SendCommand 是运维内部接口不经 DS 面网关,不受影响。
func (s *Service) SetDSCallbackGuard(g *middleware.DSCallbackGuard) { s.dsGuard = g }

// EnableRedisAuthority 打开 GM DS 写接口的 Model B active 门。开启后 legacy/nil credential
// 一律在 RPOP/审计日志前拒绝，不存在 permissive fallback。
func (s *Service) EnableRedisAuthority(repo data.BattleAuthRepo) error {
	if repo == nil {
		return errcode.New(errcode.ErrInvalidState, "gm Model B requires battle auth repo")
	}
	s.battleAuth = repo
	s.modelB = true
	return nil
}

// SendCommand 运维 / GM 工具下发一条 GM 指令。
// 默认保持兼容行为:入队即返回;带 x-pandora-gm-wait-execution-ack=1 的内部调用
// 则在有界 deadline 内等到 DS Ack,只把真实执行成功映射为 OK。
//
// ⚠️ 非业务幂等:idempotency_key 每次现生成,重复调用会入队多条(各自新 key)→ 重复发放。
// 防重复发放靠调用方不重复下发,本服务不代劳(见包注释「送达语义」)。
func (s *Service) SendCommand(ctx context.Context, req *gmv1.SendCommandRequest) (*gmv1.SendCommandResponse, error) {
	if req.GetMatchId() == 0 {
		return &gmv1.SendCommandResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}

	cmd := &gmv1.GmCommand{
		IdempotencyKey: uuid.NewString(),
		MatchId:        req.GetMatchId(),
		CreatedAtMs:    time.Now().UnixMilli(),
	}

	// 目前只支持 AddItem;后续新增指令类型在此扩展 oneof 分支。
	switch p := req.GetPayload().(type) {
	case *gmv1.SendCommandRequest_AddItem:
		ai := p.AddItem
		// player_id 是必填目标业务 ID(Snowflake uint64),0 视为漏填直接拒;bag_type 越界拒。
		if ai == nil || ai.GetPlayerId() == 0 || ai.GetConfigId() == 0 || ai.GetCount() <= 0 ||
			ai.GetBagType() < 0 || ai.GetBagType() > maxBagType {
			return &gmv1.SendCommandResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
		}
		cmd.Payload = &gmv1.GmCommand_AddItem{AddItem: &gmv1.AddItemCommand{
			PlayerId: ai.GetPlayerId(),
			ConfigId: ai.GetConfigId(),
			Count:    ai.GetCount(),
			BagType:  ai.GetBagType(),
		}}
	default:
		return &gmv1.SendCommandResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}

	// 前置校验目标对局是否有活跃战斗镜像:typo / 已结束的 match_id 立即拒(fail-fast),
	// 避免静默入僵尸队列(仅靠 30min TTL 兜底清理,运维得不到反馈)。镜像读失败则 fail-open
	// (放行 + 记 warn),避免 Redis 抖动阻断正常 GM 运维。battleChecker 为 nil 时跳过此校验。
	if s.battleChecker != nil {
		if _, found, cerr := s.battleChecker.GetBattle(ctx, req.GetMatchId()); cerr != nil {
			s.helper.Warnw("msg", "gm_command_liveness_check_failed",
				"err", cerr, "match_id", req.GetMatchId(), "hint", "fail-open,仍入队")
		} else if !found {
			s.helper.Warnw("msg", "gm_command_match_not_found",
				"match_id", req.GetMatchId(), "hint", "无活跃战斗镜像,拒绝入队(match_id 是否写错/对局已结束?)")
			return &gmv1.SendCommandResponse{Code: commonv1.ErrCode_ERR_NOT_FOUND}, nil
		}
	}

	payload, err := proto.Marshal(cmd)
	if err != nil {
		s.helper.Errorw("msg", "gm_command_marshal_failed", "err", err, "match_id", req.GetMatchId())
		return &gmv1.SendCommandResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
	}

	key := queueKey(req.GetMatchId())
	waitForExecutionAck := shouldWaitForExecutionAck(ctx)
	// LPUSH 入队 + LTRIM 限长(保留最新 maxQueueLen 条,超出丢最旧)+ EXPIRE 续命,一次 pipeline。
	pipe := s.rdb.TxPipeline()
	pipe.LPush(ctx, key, payload)
	pipe.LTrim(ctx, key, 0, maxQueueLen-1)
	pipe.Expire(ctx, key, queueTTL)
	if waitForExecutionAck {
		// 与入队同一 MULTI/EXEC:不存在 DS 先取走并 Ack、pending 却还没有可见的窗口。
		pipe.Set(ctx, executionAckResultKey(req.GetMatchId(), cmd.IdempotencyKey),
			executionAckPendingValue, executionAckTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		s.helper.Errorw("msg", "gm_command_enqueue_failed", "err", err,
			"match_id", req.GetMatchId(), "idempotency_key", cmd.IdempotencyKey)
		return &gmv1.SendCommandResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
	}

	s.helper.Infow("msg", "gm_command_enqueued",
		"match_id", req.GetMatchId(), "idempotency_key", cmd.IdempotencyKey, "type", "add_item")

	resp := &gmv1.SendCommandResponse{Code: commonv1.ErrCode_OK, IdempotencyKey: cmd.IdempotencyKey}
	if !waitForExecutionAck {
		return resp, nil
	}
	resp.Code = s.waitForExecutionAck(ctx, req.GetMatchId(), cmd.IdempotencyKey)
	return resp, nil
}

// PollCommands 战斗 DS 拉取自己这局待执行指令(RPOP,取即出队,FIFO)。
func (s *Service) PollCommands(ctx context.Context, req *gmv1.PollCommandsRequest) (*gmv1.PollCommandsResponse, error) {
	if req.GetMatchId() == 0 {
		return &gmv1.PollCommandsResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	var identity data.BattleCredentialIdentity
	if s.modelB {
		if req.GetDsPodName() == "" {
			return &gmv1.PollCommandsResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
		}
		var err error
		identity, err = s.modelBCredential(ctx, req.GetMatchId(), req.GetDsPodName())
		if err != nil {
			return &gmv1.PollCommandsResponse{Code: commonv1.ErrCode(errcode.As(err))}, nil
		}
	} else {
		// DS 回调令牌校验:只能拉自己这局的指令(防拿 A 局令牌偷 B 局 GM 指令,取即出队=窃取+丢失)。
		if err := s.dsGuard.Check(ctx, middleware.DSScope{
			Type: auth.DSTypeBattle, MatchID: req.GetMatchId(), RequireToken: true,
		}); err != nil {
			return &gmv1.PollCommandsResponse{Code: commonv1.ErrCode(errcode.As(err))}, nil
		}
	}

	max := int(req.GetMax())
	if max <= 0 {
		max = defaultPollMax
	}
	if max > maxPollMax {
		max = maxPollMax
	}

	// Model B 把完整 active tuple 校验与 RPOP 放进 auth+queue 同 slot 的一次事务，
	// 消灭 check 后轮换/吊销仍然弹出命令的 TOCTOU；legacy 保持原 RPOP。
	var raw []string
	var err error
	if s.modelB {
		raw, err = s.battleAuth.PopCommandsIfActive(
			ctx, req.GetMatchId(), identity, queueKey(req.GetMatchId()), int64(max))
	} else {
		raw, err = s.rdb.RPopCount(ctx, queueKey(req.GetMatchId()), max).Result()
	}
	if err != nil && err != redis.Nil {
		if code := errcode.As(err); code == errcode.ErrUnauthorized || code == errcode.ErrPermissionDeny {
			return &gmv1.PollCommandsResponse{Code: commonv1.ErrCode(code)}, nil
		}
		s.helper.Errorw("msg", "gm_command_poll_failed", "err", err, "match_id", req.GetMatchId())
		return &gmv1.PollCommandsResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
	}

	commands := make([]*gmv1.GmCommand, 0, len(raw))
	for _, item := range raw {
		cmd := &gmv1.GmCommand{}
		if uerr := proto.Unmarshal([]byte(item), cmd); uerr != nil {
			// 单条损坏不影响其余;丢弃并记日志(已出队,不会再投递)。
			s.helper.Warnw("msg", "gm_command_unmarshal_failed", "err", uerr, "match_id", req.GetMatchId())
			continue
		}
		commands = append(commands, cmd)
	}

	if len(commands) > 0 {
		s.helper.Infow("msg", "gm_commands_delivered",
			"match_id", req.GetMatchId(), "count", len(commands), "ds_pod", req.GetDsPodName())
	}
	return &gmv1.PollCommandsResponse{Code: commonv1.ErrCode_OK, Commands: commands}, nil
}

// AckCommand 战斗 DS 回报执行结果。旧的非等待 SendCommand 仍只记审计;
// 等待型调用则以首次 Ack 为不可变终态,唤醒对应的 SendCommand。
func (s *Service) AckCommand(ctx context.Context, req *gmv1.AckCommandRequest) (*gmv1.AckCommandResponse, error) {
	if req.GetMatchId() == 0 || req.GetIdempotencyKey() == "" ||
		len(req.GetIdempotencyKey()) > maxAckIdempotencyKeyBytes {
		return &gmv1.AckCommandResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if s.modelB {
		identity, err := s.modelBCredential(ctx, req.GetMatchId(), "")
		if err != nil {
			return &gmv1.AckCommandResponse{Code: commonv1.ErrCode(errcode.As(err))}, nil
		}
		if err := s.battleAuth.CheckActive(ctx, req.GetMatchId(), identity); err != nil {
			return &gmv1.AckCommandResponse{Code: commonv1.ErrCode(errcode.As(err))}, nil
		}
	} else if err := s.dsGuard.Check(ctx, middleware.DSScope{
		Type: auth.DSTypeBattle, MatchID: req.GetMatchId(), RequireToken: true,
	}); err != nil {
		return &gmv1.AckCommandResponse{Code: commonv1.ErrCode(errcode.As(err))}, nil
	}
	ack := executionAck{OK: req.GetOk(), Message: boundedAckMessage(req.GetMessage())}
	payload, err := json.Marshal(ack)
	if err != nil {
		s.helper.Errorw("msg", "gm_command_ack_marshal_failed", "err", err,
			"match_id", req.GetMatchId(), "idempotency_key", req.GetIdempotencyKey())
		return &gmv1.AckCommandResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
	}

	stored, err := storeExecutionAckScript.Run(ctx, s.rdb, []string{
		executionAckResultKey(req.GetMatchId(), req.GetIdempotencyKey()),
		executionAckSignalKey(req.GetMatchId(), req.GetIdempotencyKey()),
	}, executionAckPendingValue, payload, executionAckTTL.Milliseconds()).Text()
	if err != nil && err != redis.Nil {
		s.helper.Errorw("msg", "gm_command_ack_store_failed", "err", err,
			"match_id", req.GetMatchId(), "idempotency_key", req.GetIdempotencyKey())
		return &gmv1.AckCommandResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
	}
	if err == nil && stored != string(payload) {
		var first executionAck
		if uerr := json.Unmarshal([]byte(stored), &first); uerr != nil {
			s.helper.Errorw("msg", "gm_command_ack_state_corrupt", "err", uerr,
				"match_id", req.GetMatchId(), "idempotency_key", req.GetIdempotencyKey())
			return &gmv1.AckCommandResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
		}
		s.helper.Warnw("msg", "gm_command_ack_conflict",
			"match_id", req.GetMatchId(), "idempotency_key", req.GetIdempotencyKey(),
			"first_ok", first.OK, "retry_ok", ack.OK)
		return &gmv1.AckCommandResponse{Code: commonv1.ErrCode_ERR_INVALID_STATE}, nil
	}

	if ack.OK {
		s.helper.Infow("msg", "gm_command_acked",
			"match_id", req.GetMatchId(), "idempotency_key", req.GetIdempotencyKey(), "ok", true)
	} else {
		s.helper.Warnw("msg", "gm_command_acked",
			"match_id", req.GetMatchId(), "idempotency_key", req.GetIdempotencyKey(), "ok", false, "reason", ack.Message)
	}
	return &gmv1.AckCommandResponse{Code: commonv1.ErrCode_OK}, nil
}

func shouldWaitForExecutionAck(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, value := range md.Get(waitExecutionAckMetadataKey) {
		if value == waitExecutionAckMetadataValue {
			return true
		}
	}
	return false
}

func (s *Service) waitForExecutionAck(ctx context.Context, matchID uint64, idempotencyKey string) commonv1.ErrCode {
	waitCtx, cancel := context.WithTimeout(ctx, executionAckWaitTimeout)
	defer cancel()

	resultKey := executionAckResultKey(matchID, idempotencyKey)
	signalKey := executionAckSignalKey(matchID, idempotencyKey)
	payload, err := s.rdb.Get(waitCtx, resultKey).Result()
	if err != nil {
		if err == redis.Nil {
			s.helper.Errorw("msg", "gm_command_execution_marker_missing",
				"match_id", matchID, "idempotency_key", idempotencyKey)
			return commonv1.ErrCode_ERR_INTERNAL
		}
		return s.executionAckWaitErrorCode(waitCtx, matchID, idempotencyKey, err)
	}

	if payload == executionAckPendingValue {
		for {
			if waitErr := waitCtx.Err(); waitErr != nil {
				return s.executionAckWaitErrorCode(waitCtx, matchID, idempotencyKey, waitErr)
			}
			wait := executionAckBlockSlice
			if deadline, ok := waitCtx.Deadline(); ok {
				remaining := time.Until(deadline)
				if remaining <= 0 {
					return s.executionAckWaitErrorCode(waitCtx, matchID, idempotencyKey, context.DeadlineExceeded)
				}
				if remaining < wait {
					// Redis BLPOP 的最小单位是 1s;向上取整,返回后再以 context
					// 权威 deadline 判定超时,最多额外占用不超过一个片段。
					wait = executionAckBlockSlice
				}
			}
			popped, popErr := s.rdb.BLPop(waitCtx, wait, signalKey).Result()
			if popErr == redis.Nil {
				// 一个阻塞片段内无事件:只重查 context deadline/cancel,
				// 不 GET/轮询 Ack 状态。
				continue
			}
			if popErr != nil {
				return s.executionAckWaitErrorCode(waitCtx, matchID, idempotencyKey, popErr)
			}
			if len(popped) != 2 {
				s.helper.Errorw("msg", "gm_command_execution_signal_invalid",
					"match_id", matchID, "idempotency_key", idempotencyKey, "parts", len(popped))
				return commonv1.ErrCode_ERR_INTERNAL
			}
			payload = popped[1]
			break
		}
	}

	var ack executionAck
	if err := json.Unmarshal([]byte(payload), &ack); err != nil {
		s.helper.Errorw("msg", "gm_command_execution_result_corrupt", "err", err,
			"match_id", matchID, "idempotency_key", idempotencyKey)
		return commonv1.ErrCode_ERR_INTERNAL
	}
	if !ack.OK {
		// SendCommandResponse 的现有 proto 没有 message 字段;用内部 gRPC binary trailer
		// 透传已限长失败原因,旧客户端忽略 trailer 仍保持兼容。
		if ack.Message != "" {
			_ = grpc.SetTrailer(ctx, metadata.Pairs(executionAckMessageMetadataKey, ack.Message))
		}
		s.helper.Warnw("msg", "gm_command_execution_failed",
			"match_id", matchID, "idempotency_key", idempotencyKey, "reason", ack.Message)
		return commonv1.ErrCode_ERR_INVALID_STATE
	}
	s.helper.Infow("msg", "gm_command_execution_confirmed",
		"match_id", matchID, "idempotency_key", idempotencyKey)
	return commonv1.ErrCode_OK
}

func (s *Service) executionAckWaitErrorCode(
	ctx context.Context,
	matchID uint64,
	idempotencyKey string,
	err error,
) commonv1.ErrCode {
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		s.helper.Warnw("msg", "gm_command_execution_wait_canceled",
			"match_id", matchID, "idempotency_key", idempotencyKey)
		return commonv1.ErrCode_ERR_CANCELED
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || err == redis.Nil {
		s.helper.Warnw("msg", "gm_command_execution_wait_timeout",
			"match_id", matchID, "idempotency_key", idempotencyKey)
		return commonv1.ErrCode_ERR_TIMEOUT
	}
	s.helper.Errorw("msg", "gm_command_execution_wait_failed", "err", err,
		"match_id", matchID, "idempotency_key", idempotencyKey)
	return commonv1.ErrCode_ERR_INTERNAL
}

func boundedAckMessage(message string) string {
	message = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, message)
	if len(message) <= maxAckMessageBytes {
		return message
	}
	b := []byte(message)[:maxAckMessageBytes]
	for len(b) > 0 && !utf8.Valid(b) {
		b = b[:len(b)-1]
	}
	return string(b)
}

func (s *Service) modelBCredential(
	ctx context.Context,
	matchID uint64,
	podName string,
) (data.BattleCredentialIdentity, error) {
	_, verified, err := s.dsGuard.CheckBattleCredential(ctx, middleware.DSScope{
		Type: auth.DSTypeBattle, MatchID: matchID, Pod: podName, RequireToken: true,
	})
	if err != nil {
		return data.BattleCredentialIdentity{}, err
	}
	if verified == nil || verified.ExpMs <= 0 {
		return data.BattleCredentialIdentity{}, errcode.New(errcode.ErrUnauthorized,
			"battle callback requires complete Model B credential")
	}
	return data.BattleCredentialIdentity{
		PodName:       verified.Pod,
		InstanceUID:   verified.InstanceUID,
		InstanceEpoch: verified.ProtocolEpoch,
		Gen:           verified.Gen,
		JTI:           verified.JTI,
		ExpMs:         uint64(verified.ExpMs),
		Kid:           verified.Kid,
		TokenSHA256:   verified.TokenSHA256,
		WriterEpoch:   verified.WriterEpoch,
	}, nil
}
