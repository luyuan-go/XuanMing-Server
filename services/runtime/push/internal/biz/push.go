// Package biz 是 push 服务的业务逻辑层(usecase)。
//
// 2026-07-22 审计 v2(拉取式投递,Redis 缓冲 = 唯一定序与投递权威)+ v3 会话门
// (P0,INC-20260722-004)+ v4 R4 复审修复:
//   - RunSubscribeStream 是该 stream 的**唯一写者**:循环「Range(>游标) 拉缓冲投递」,
//     触发源 = 本地唤醒信号(本 Pod 消费到该玩家消息,零延迟)+ 兜底轮询(30s,只为
//     滚动窗口/多副本时落在别的 Pod 的写入;单副本稳态几乎全走信号,审计 P1 容量:
//     不能按每连接每秒打 Redis)+ 广播箱。
//   - **会话现行性门**(P0):建流时在同玩家条带锁内原子完成「校验 jti == login 会话
//     权威当前一代 + 注册连接」(AuthorizeAndRegister;R4 复审①:校验与注册分离存在
//     TOCTOU,旧会话校验通过后暂停、新会话注册、旧会话再注册会反过来顶掉新设备)。
//   - **流内会话看门狗**(R4 复审②):独立 goroutine 周期复查 jti 现行性 + token 到期,
//     不受写者 stream.Send 阻塞影响——会话失效后 ≤sessionRecheckInterval 取消流上下文,
//     写者在下一次 Send 前必然观察到取消,不再向该流投递任何新帧。写者若正阻塞在
//     Send 上,已交给传输层的至多一帧不受影响,流句柄本身由 gRPC keepalive/
//     max_connection_age 与 Envoy max_stream_duration 有界回收(诚实契约:30s 界的是
//     「停止投递+发起关流」,不是「TCP 句柄消亡」)。会话权威不可达按 fail-closed:
//     建流拒绝;流内连续 sessionFailClose 次查询失败后关流(短抖动不误杀,持续故障
//     不裸奔)。
//   - **gap 检测 fail-closed**(R4 复审 P1):每轮把缓冲拉空之后做 LostSince 终检——
//     修剪只删 score 前缀,任何「已分配但未投递就被修剪/滑出读窗」的帧在拉空时刻
//     必然表现为 LostSince(cursor) > cursor(fl 哨兵持久,跨重连不丢证据)。检测失败
//     返回错误、游标不推进(首轮 = 断流,稳态 = 退避),不允许把「查不了」当「无丢失」。
//   - 拉取失败退避重试(审计 P1:Redis 故障时每连接每秒报错 = 日志风暴):失败后按
//     指数退避(1s..60s)暂停拉取,只记首错与每 10 次;游标不动,恢复后续传不漏。
//   - v5 R5 复审(2026-07-22):①投递前会话 fencing(sessionFenceDelivery,P0-4),
//     借 Redis session key 单点串行保证「轮换后产生的私有帧旧流一帧不发」,跨 Pod 成立
//     (authRegMu 只覆盖单 Pod);②流内复查先判代际后判到期(P0-2):「已过期且已被顶」
//     必须回 ErrSessionSuperseded(→ABORTED)而非 UNAUTHENTICATED,否则被顶设备自动
//     重登反顶新设备。
//   - v6 R6 复审(2026-07-23):投递 fence 从每批收紧为**逐帧**——按批 fence 通过后轮换,
//     旧流仍会发完整批(≤maxFrames);逐帧后每条交付帧都产生于该帧自己的 fence 通过
//     之前,在途暴露 ≤1 帧(检查与 Send 无法跨存储原子,零瞬时窗口不可达,契约按此
//     诚实表述)。
package biz

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
	pushv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/push/v1"

	"github.com/luyuancpp/pandora/services/runtime/push/internal/data"
)

const (
	// pollFallbackInterval 兜底轮询周期:只兜"写入落在其他 Pod"(滚动重叠/未来多副本)
	// 的场景,本 Pod 写入走唤醒信号零等待。40 万 CCU × 1/30s ≈ 1.3 万空读/s,容量可承受
	// (审计 P1:原每连接 1s 轮询在目标容量下不成立)。
	pollFallbackInterval = 30 * time.Second
	// sessionRecheckInterval 流内会话复查周期(顶号/登出/过期后「停止投递+发起关流」
	// 的最大暴露窗;由独立看门狗驱动,不受写者 Send 阻塞影响,R4 复审②)。
	sessionRecheckInterval = 30 * time.Second
	// sessionFailClose 会话权威连续查询失败多少次后 fail-closed 关流。
	sessionFailClose = 3
	// drainBackoffMax 拉取失败的最大退避。
	drainBackoffMax = time.Minute
	// authRegStripes 「会话校验+注册」同玩家串行化条带锁数量(P0 R4 复审①)。
	authRegStripes = 64
)

// ResyncTopic 是合成 resync 信号帧的 topic(R4 P1-3 gap 闭环)。不是 kafka topic,
// 只存在于 Subscribe 下行:写者把缓冲拉空后发现客户端游标之后已有帧被修剪/滑出保留窗
// (LostSince > cursor)时,发一条该 topic 的空 payload 帧(ts_ms=0,不推进客户端游标),
// 告知客户端「增量推送已有确定丢失,须回源拉取权威态」(邮件列表、好友申请等各域
// 主动 refresh)。同一段丢失只信号一次(本地游标跳到丢失上界);此后新的丢失再次触发。
// 契约注释同步在 proto/pandora/push/v1/push.proto;UE 侧由各业务模型处理该 topic。
const ResyncTopic = "pandora.push.resync"

// SessionInfo 是建流时从 Envoy 验签 payload 头提取的会话身份(service 层注入)。
type SessionInfo struct {
	JTI   string // 会话代际;"" = 未经网关(dev 直连)
	ExpMs int64  // 会话 JWT 到期毫秒;0 = 未携带
}

// sessionClose 是看门狗写入的关流原因(atomic.Pointer 载体;非 nil 即会话已裁决关流)。
type sessionClose struct{ err error }

// PushUsecase 是 push 服务用例。
type PushUsecase struct {
	conns   *ConnectionManager
	offline data.OfflineCacheRepo

	sessionGate    data.SessionGate // nil = 未装配(dev 裸跑)
	requireSession bool             // true = 生产档:无 jti / 无权威一律拒(fail-closed)

	// authRegMu 同玩家「会话门校验 + Register」的串行化条带锁(P0 R4 复审①:
	// 校验与注册之间的 TOCTOU 窗口内,旧会话可在新会话注册后再注册并顶掉新设备)。
	authRegMu [authRegStripes]sync.Mutex

	// sessionRecheckEvery 看门狗复查周期(缺省 sessionRecheckInterval;测试注入短周期)。
	sessionRecheckEvery time.Duration
}

// NewPushUsecase 构造 PushUsecase。
func NewPushUsecase(conns *ConnectionManager, offline data.OfflineCacheRepo) *PushUsecase {
	return &PushUsecase{conns: conns, offline: offline, sessionRecheckEvery: sessionRecheckInterval}
}

// SetSessionGate 注入会话现行性权威只读视图(main 装配;require = 生产强制档,
// prod 生成器机械置 true,INC-20260722-004)。
func (u *PushUsecase) SetSessionGate(gate data.SessionGate, require bool) {
	u.sessionGate = gate
	u.requireSession = require
}

// Conns 暴露 ConnectionManager,给 service 层 Unregister 用。
func (u *PushUsecase) Conns() *ConnectionManager {
	return u.conns
}

// AuthorizeAndRegister 在同玩家条带锁内串行执行「会话门校验 → 注册连接」
// (P0,INC-20260722-004 R4 复审①)。锁保证同一玩家的校验与注册不可交错:
//   - 旧 token 的校验若排在新会话注册**之后**,必然读到已轮换的 jti 而被拒;
//   - 若排在**之前**(新会话的 login 轮换尚未发生),旧流短暂注册,但新会话随后
//     注册时按顶号语义取消它——新 token 的签发(login 轮换 jti)先于新连接建流,
//     故新连接的校验必过、注册必成。
//
// 两种交错都收敛到「新会话持有连接槽」,旧 token 不再有任何顶掉新设备的窗口。
// 锁内含一次会话权威(Redis)读:有界、按 64 条带分散,仅建流路径竞争。
func (u *PushUsecase) AuthorizeAndRegister(
	ctx context.Context,
	playerID uint64,
	sess SessionInfo,
	stream PushStream,
	closeFn func(),
) (*StreamSlot, error) {
	mu := &u.authRegMu[playerID%authRegStripes]
	mu.Lock()
	defer mu.Unlock()

	if err := u.AuthorizeSubscribe(ctx, playerID, sess); err != nil {
		return nil, err
	}
	return u.conns.Register(playerID, stream, closeFn), nil
}

// AuthorizeSubscribe 建流会话门(P0):请求携带的 jti 必须是该玩家当前一代会话。
// 旧/被顶号 token 在 exp 前仍能过 Envoy 验签,现行性只能问会话权威(§9.23)。
// 权威不可达 → ErrUnavailable(fail-closed 拒建流,客户端退避重试)。
// 建流路径必须经 AuthorizeAndRegister 调用(与注册同锁,防 TOCTOU);单独暴露仅供测试。
func (u *PushUsecase) AuthorizeSubscribe(ctx context.Context, playerID uint64, sess SessionInfo) error {
	if playerID == 0 {
		if u.requireSession {
			return errcode.New(errcode.ErrUnauthorized, "subscribe requires authenticated player")
		}
		return nil // dev 匿名直连(生产必经 Envoy jwt_authn,player_id 恒非 0)
	}
	if u.sessionGate == nil {
		if u.requireSession {
			return errcode.New(errcode.ErrUnavailable, "session authority not wired; subscribe rejected (fail-closed)")
		}
		return nil
	}
	if sess.JTI == "" {
		if u.requireSession {
			// 生产必经 :8443 jwt_authn,payload 头必然存在;缺失 = 绕网关,fail-closed。
			return errcode.New(errcode.ErrUnauthorized, "session payload required")
		}
		return nil // dev 直连内网端口联调
	}
	cur, found, err := u.sessionGate.CurrentJTI(ctx, playerID)
	if err != nil {
		return err // ErrUnavailable(fail-closed)
	}
	if !found {
		return errcode.New(errcode.ErrUnauthorized, "session expired or logged out; login again")
	}
	if cur != sess.JTI {
		// 顶号用专属码(→ gRPC ABORTED):与自然过期/登出的 ErrUnauthorized 可判别,
		// 被顶设备不得自动完整 Login 反顶新设备(INC-20260722-004 R4 P0 互踢循环)。
		plog.With(ctx).Warnw("msg", "push_subscribe_superseded_rejected", "player_id", playerID)
		return errcode.New(errcode.ErrSessionSuperseded, "session superseded by a newer login")
	}
	return nil
}

// recheckSession 流内会话复查:被顶号/登出/到期 → 返回不可恢复错误(关流);
// 权威查询失败 → 返回 (retryable=true) 由调用方计连败。
//
// 裁决顺序(R5 复审 P0-2):**先判会话代际,后判到期**。旧顺序先判 ExpMs,
// 「A 已过期且已被 B 顶号」只得到 ErrUnauthorized(→UNAUTHENTICATED),UE 把它当
// 自然过期用缓存凭据自动完整 Login,轮换 jti 反顶 B——错误码 14 的互踢防护被
// 到期分支短路。代际不匹配必须无条件优先返回 ErrSessionSuperseded(→ABORTED);
// 只有确认 jti 仍是当前一代(自己就是唯一会话,重登无反顶对象)才按到期关流。
func (u *PushUsecase) recheckSession(ctx context.Context, playerID uint64, sess SessionInfo) (retryable bool, err error) {
	if u.sessionGate != nil && playerID != 0 && sess.JTI != "" {
		cur, found, gerr := u.sessionGate.CurrentJTI(ctx, playerID)
		if gerr != nil {
			return true, gerr
		}
		if !found {
			return false, errcode.New(errcode.ErrUnauthorized, "session logged out; stream closed")
		}
		if cur != sess.JTI {
			// 顶号专属码,客户端据 ABORTED 转交互登录而非自动重登(R4 P0 互踢循环)。
			return false, errcode.New(errcode.ErrSessionSuperseded, "session superseded; stream closed")
		}
	}
	// 至此 jti 仍是当前一代(或 dev 无 gate/无 jti,建流时已按 require 档裁决):
	// 到期按普通未授权关流,客户端自动换新会话不构成反顶。
	if sess.ExpMs > 0 && time.Now().UnixMilli() >= sess.ExpMs {
		return false, errcode.New(errcode.ErrUnauthorized, "session token expired; stream closed")
	}
	return false, nil
}

// sessionFenceDelivery 投递前会话 fencing(R5 复审 P0-4,跨 Pod TOCTOU 收口):
// 进程内 authRegMu 只能串行化**同一 Pod** 的「校验+注册」;滚动/多副本时,Pod A 可在
// 读到旧 jti 后暂停,B 于 Pod B 登录轮换并建流,A 恢复注册并开始投递——旧流在看门狗
// 复查周期(≤30s)内持续读取私有帧。
//
// 收口依据 Redis 单点(单 key)串行:会话轮换 = 对 pandora:sess:<pid> 的一次写。
// 任何「轮换后产生」的私有帧,其入缓冲写必然发生在轮换之后;能读到该帧的 Range
// 必然完成于该写之后;本 fence 在该帧 Send 前发起(R6 起逐帧),对同一 session key
// 的读必然排在轮换写之后 → 必然观察到新 jti 而拒绝。因此**轮换后产生的帧零交付**,
// 单 Pod、多 Pod、Redis Cluster(fence 与轮换同 key,per-key 串行)下均成立;
// 轮换前已产生、且本帧 fence 已通过的帧仍可能由在途 Send 交付(≤1 帧,被顶前会话
// 自己的合法数据)——「轮换瞬间起零帧」跨存储不可达,不作此宣称。
//
// 返回错误语义与 recheckSession 对齐:顶号 ErrSessionSuperseded / 登出 ErrUnauthorized
// (不可恢复,调用方关流);权威不可达返回原 ErrUnavailable(fail-closed 不投递,
// 调用方按拉取失败退避,游标不动不漏帧)。
func (u *PushUsecase) sessionFenceDelivery(ctx context.Context, playerID uint64, sess SessionInfo) error {
	if u.sessionGate == nil || playerID == 0 || sess.JTI == "" {
		return nil // dev 裸跑/无 jti:建流时已按 require 档裁决
	}
	cur, found, err := u.sessionGate.CurrentJTI(ctx, playerID)
	if err != nil {
		return err // ErrUnavailable:查不了 ≠ 可投递(§9.22 fail-closed)
	}
	if !found {
		return errcode.New(errcode.ErrUnauthorized, "session logged out; delivery fenced")
	}
	if cur != sess.JTI {
		plog.With(ctx).Warnw("msg", "push_delivery_fenced_superseded", "player_id", playerID)
		return errcode.New(errcode.ErrSessionSuperseded, "session superseded; delivery fenced")
	}
	return nil
}

// isSessionFenceClose 判定 drainBuffer 返回的错误是否为「会话已失效」类(不可恢复,
// 必须关流),与「拉取/权威瞬时失败」类(退避重试)区分。
func isSessionFenceClose(err error) bool {
	switch errcode.As(err) {
	case errcode.ErrSessionSuperseded, errcode.ErrUnauthorized:
		return true
	}
	return false
}

// drainBuffer 把投递缓冲中游标 > cursor 的帧全部投递,随后做 gap 终检,返回推进后的游标。
// 拉取、发送或 gap 检查失败返回 (当前游标, err):游标不推进,下次重试不漏。
//
// gap 检查分两层(R7 复审 P1-1 收口):
//   - 每页发送前预检:resync 信号必须先于越过缺口的帧到达客户端,否则多页补推期间
//     断流重连会让客户端 last_seen_ms 越过缺口,缺口从此永不回补(详见循环内注释);
//   - 拉空后终检(R4 复审 P1-2,fail-closed):兜住「最后一页预检后~拉空」间隙内被
//     修剪/滑出读窗而未投递的帧——fl 哨兵持久,跨重连不丢证据。
//
// 检测失败返回错误(不得当「无丢失」继续,否则游标越过缺口后 resync 永远无法触发)。
// 检出丢失:发一帧 resync 信号(ts_ms=0,不推进客户端游标),基线跳到丢失上界,
// 同一段丢失只信号一次;此后 fl 再次越过基线(新的丢失)会再次触发。
// cursor=0(首连拉空且缓冲无帧)不检:新客户端无增量历史,交付契约从当下开始。
// 该跳过的安全性依赖客户端时序契约(R9 复审 P1-e,已写入 push.proto):登录成功提交点
// 先订阅 push、后发起任何业务域快照拉取(UE 侧 MyAccountModel 是唯一订阅点),订阅前
// 被修剪的事件必被订阅后的首次全量快照覆盖;客户端若先拉快照再订阅,此跳过不再安全。
//
// 会话 fencing(R5 复审 P0-4;R6 复审收紧为**逐帧**):每帧 Send 之前复核会话现行性
// (sessionFenceDelivery)。R6 复审确认按批 fence 存在批内竞态——fence 通过后轮换,
// 旧流仍会把整批(最多 maxFrames)帧发完。逐帧复核后的诚实上界:**每一条交付帧都
// 产生于该帧自己的 fence 通过之前**(帧写入 → Range 读到 → fence 排在轮换后必失败,
// Redis session key 单点串行),轮换后产生的帧零交付;检查与 Send 之间的固有在途
// 暴露收敛到 ≤1 帧(跨存储无事务性 Send,零瞬时窗口不可达,契约按此表述,不夸大)。
// 空批不查(闲置轮询零额外权威读);查询失败按拉取失败语义返回(fail-closed 不投递,
// 游标不动);顶号/登出错误由调用方判别关流。逐帧一次 HGET 的代价只在有帧可投时发生,
// 稳态推送低频(每玩家每秒 0~几条),重连补推最坏 512 帧一次性多 512 次点查,可承受。
func (u *PushUsecase) drainBuffer(ctx context.Context, slot *StreamSlot, playerID uint64, cursor int64, sess SessionInfo) (int64, error) {
	// entry 是本轮进入时的游标:gap 检查的初始基线(R5 复审 P1-1,见拉空后注释)。
	entry := cursor
	// baseline 随已信号的丢失上界推进,同一段丢失只信号一次;首连(entry<=0)从首页
	// 交付上界开始。lostBound 记录本轮已信号丢失的最大上界,拉空后一次性推进游标。
	baseline := entry
	var lostBound int64
	for {
		if err := ctx.Err(); err != nil {
			return cursor, nil
		}
		frames, err := u.offline.Range(ctx, playerID, cursor)
		if err != nil {
			return cursor, err
		}
		if len(frames) == 0 {
			break
		}
		// 每页发送前 gap 预检(R7 复审 P1-1):resync 信号必须**先于**越过缺口的帧
		// 到达客户端。旧实现只在拉空后终检,多页补推期间客户端游标已被幸存帧推过
		// 缺口;若 resync 帧发出前流断开重连,新流 last_seen_ms 已越过缺口,而服务端
		// 本地游标未持久化、重连后按客户端游标重建基线,fl 哨兵证据虽在却再也不会
		// 触发针对该缺口的信号——permanent miss。先信号后发帧,任何时刻断流客户端
		// 要么未越过缺口(重连回补),要么已收到 resync(全量校正),漏报窗口闭合。
		// 预检失败按拉取失败语义返回:fail-closed,游标不动,不越过潜在缺口。
		if baseline > 0 {
			lost, lerr := u.offline.LostSince(ctx, playerID, baseline)
			if lerr != nil {
				return cursor, lerr
			}
			if lost > baseline {
				plog.With(ctx).Warnw("msg", "push_gap_resync_signaled",
					"player_id", playerID, "baseline", baseline, "cursor", cursor, "lost_up_to", lost)
				if serr := slot.stream.Send(&pushv1.PushFrame{Topic: ResyncTopic}); serr != nil {
					return cursor, serr
				}
				baseline = lost
				if lost > lostBound {
					lostBound = lost
				}
			}
		}
		for _, f := range frames {
			if err := ctx.Err(); err != nil {
				return cursor, nil
			}
			// 逐帧 fence(R6):轮换发生在批内任意点,后续帧一律不发;已发帧均产生于
			// 各自 fence 通过之前。失败时游标停在最后一条已交付帧,不漏不重。
			if ferr := u.sessionFenceDelivery(ctx, playerID, sess); ferr != nil {
				return cursor, ferr
			}
			if serr := slot.stream.Send(f.Frame); serr != nil {
				return cursor, serr
			}
			cursor = f.ScoreMs
		}
		if baseline <= 0 {
			// 首连:无增量历史,契约从首页交付上界开始,后续页起才做预检。
			baseline = cursor
		}
	}
	// gap 终检兜底(R5 复审 P1-2):覆盖「最后一页预检后~拉空」间隙内新发生的修剪。
	// 基线用随信号推进的 baseline 而非拉空后游标(R5 复审 P1-1):丢失帧与幸存帧
	// 交错时(如丢 1001、幸存 1002),拉空后游标已被幸存帧推进到 1002,fl=1001 不再
	// 大于游标——丢失会被幸存帧"掩护"成永久漏报。已信号段(baseline 已跳到丢失上界)
	// 不重复信号;首连且缓冲无帧(baseline 仍 0)连检测都不做。
	// 检测失败返回错误(不得当「无丢失」继续,否则游标越过缺口后 resync 永远无法触发)。
	if baseline <= 0 || ctx.Err() != nil {
		return cursor, nil
	}
	lost, lerr := u.offline.LostSince(ctx, playerID, baseline)
	if lerr != nil {
		return cursor, lerr
	}
	if lost > baseline {
		plog.With(ctx).Warnw("msg", "push_gap_resync_signaled",
			"player_id", playerID, "baseline", baseline, "cursor", cursor, "lost_up_to", lost)
		if serr := slot.stream.Send(&pushv1.PushFrame{Topic: ResyncTopic}); serr != nil {
			return cursor, serr
		}
		if lost > lostBound {
			lostBound = lost
		}
	}
	// 游标跳到已信号丢失上界:防止下一轮把同一段丢失再次当缺口信号。
	if lostBound > cursor {
		cursor = lostBound
	}
	return cursor, nil
}

// drainBackoff 按连败次数给退避时长(1s,2s,4s..封顶 drainBackoffMax)。
func drainBackoff(streak int) time.Duration {
	shift := streak - 1
	if shift > 6 {
		shift = 6
	}
	d := time.Second << uint(shift)
	if d > drainBackoffMax {
		return drainBackoffMax
	}
	return d
}

// RunSubscribeStream 跑一个 Subscribe stream 的生命周期(本 goroutine = 唯一写者)。
// afterCursor = 客户端 last_seen_ms(0 = 首连,从缓冲现存帧开始拉)。
// sess = 建流会话身份(AuthorizeAndRegister 已过门;流内由独立看门狗周期复查)。
func (u *PushUsecase) RunSubscribeStream(
	ctx context.Context,
	slot *StreamSlot,
	playerID uint64,
	afterCursor int64,
	sess SessionInfo,
) error {
	h := plog.With(ctx)
	ctx, cancelStream := context.WithCancel(ctx)

	var sessClose atomic.Pointer[sessionClose]
	var wg sync.WaitGroup
	// defer LIFO:先 cancelStream 让看门狗退出,再 wg.Wait 保证其不逃逸出请求生命周期(§16.7)。
	defer wg.Wait()
	defer cancelStream()

	// 会话复查看门狗(P0 R4 复审②):独立于写者 goroutine——写者可能长时间阻塞在
	// stream.Send(慢客户端流控),旧实现把复查放在写者 select 里,阻塞期间会话失效
	// 无人裁决,「30s 内关闭旧流」不成立。看门狗只读会话权威并取消流上下文,不碰
	// stream.Send(单写者不变量保持);取消后写者在下一次 Send 前必然观察到并停止投递。
	wg.Add(1)
	go func() {
		defer wg.Done()
		// panic 兜底(压测审核【必修-6】):看门狗 panic 此前会崩整进程(全 Pod 长连瞬断)。
		// recover 后看门狗终止,流失去会话复查驱动 → 主动 cancelStream 断本流,客户端重连
		// 重建看门狗,fail-closed 不留"无人裁决会话"的流。
		defer cancelStream()
		defer safego.Recover(ctx, "push_session_watchdog")
		tick := time.NewTicker(u.sessionRecheckEvery)
		defer tick.Stop()
		fails := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			retryable, serr := u.recheckSession(ctx, playerID, sess)
			if serr == nil {
				fails = 0
				continue
			}
			if retryable {
				fails++
				if fails < sessionFailClose {
					continue
				}
				// 会话权威持续不可达:fail-closed 关流(重连时建流门同样 fail-closed,
				// 客户端退避;不允许在无法证明现行性的情况下长期裸奔,审计 P0)。
				h.Errorw("msg", "push_stream_session_authority_down_fail_closed",
					"player_id", playerID, "fails", fails, "err", serr)
			} else {
				// 顶号/登出/到期:关流(跨 Pod 旧流同样在此收口)。
				h.Warnw("msg", "push_stream_session_closed", "player_id", playerID, "err", serr)
			}
			sessClose.Store(&sessionClose{err: serr})
			cancelStream()
			return
		}
	}()

	// exit 统一收敛返回值:看门狗触发的取消要把会话错误作为关流原因带给客户端
	// (经 errcode gRPC 映射成 UNAUTHENTICATED/UNAVAILABLE,客户端据此决定换会话或退避)。
	exit := func(err error) error {
		if p := sessClose.Load(); p != nil {
			return p.err
		}
		return err
	}

	cursor := afterCursor

	// 首轮拉取(重连补推/首连拉缓冲现存帧;gap 终检在 drainBuffer 拉空后执行)。
	if playerID > 0 {
		next, err := u.drainBuffer(ctx, slot, playerID, cursor, sess)
		if err != nil {
			h.Warnw("msg", "push_replay_failed_stream_closed", "player_id", playerID, "cursor", cursor, "err", err)
			return exit(err) // 首轮失败断流:客户端重连重试(游标没动,不漏)
		}
		if next > cursor {
			h.Debugw("msg", "push_replayed", "player_id", playerID, "after_cursor", afterCursor, "cursor", next)
		}
		cursor = next
	}

	poll := time.NewTicker(pollFallbackInterval)
	defer poll.Stop()

	var (
		drainFailStreak int
		drainRetryAt    time.Time
	)
	for {
		var pull bool
		select {
		case <-ctx.Done():
			return exit(nil)
		case <-slot.notify:
			pull = true
		case <-poll.C:
			pull = true
		case bf := <-slot.bcast:
			if ctx.Err() != nil {
				return exit(nil) // 会话已失效/流已取消:不得再投任何帧
			}
			if err := slot.stream.Send(bf); err != nil {
				h.Warnw("msg", "push_broadcast_send_failed", "player_id", playerID, "err", err)
				return exit(err)
			}
		}
		if pull && playerID > 0 && time.Now().After(drainRetryAt) {
			next, err := u.drainBuffer(ctx, slot, playerID, cursor, sess)
			if err != nil {
				// 会话已失效(顶号/登出,R5 复审 P0-4 投递 fence 检出):立即关流,
				// 不得退避重试——旧流不允许保留任何后续投递机会,关流原因保留
				// 可判别码(ABORTED/UNAUTHENTICATED)交给客户端裁决。
				if isSessionFenceClose(err) {
					h.Warnw("msg", "push_stream_closed_by_delivery_fence", "player_id", playerID, "err", err)
					return exit(err)
				}
				// 拉取/gap 终检/权威瞬时失败不断流:游标未动,退避后重试(实时降级为
				// 轮询迟延;只记首错与每 10 次,防 Redis 故障日志风暴)。
				drainFailStreak++
				drainRetryAt = time.Now().Add(drainBackoff(drainFailStreak))
				if drainFailStreak == 1 || drainFailStreak%10 == 0 {
					h.Warnw("msg", "push_drain_failed_backoff",
						"player_id", playerID, "cursor", cursor, "streak", drainFailStreak, "err", err)
				}
				continue
			}
			drainFailStreak = 0
			drainRetryAt = time.Time{}
			cursor = next
		}
	}
}
