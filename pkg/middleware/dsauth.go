// Package middleware — DS 回调服务令牌守卫(审核 P1 #1,2026-07-10 拍板落地)。
//
// 背景:DS 面网关 :8444 只有方法白名单 + NetworkPolicy 网络隔离,7 个 DS 回调方法
// (hub/ds Heartbeat、GmService Poll/Ack、locator SetLocation/ReportDisconnect、
// battle_result ReportResult)此前不认证调用者身份——被攻破的 DS / 集群内伪造者可以
// 冒充任意对局 / 任意分片上报(伪造结算、篡改玩家位置、偷 GM 指令)。
//
// 机制(docs/design/decision-revisit-ds-callback-auth.md):
//   - ds_allocator / hub_allocator 分配 / 发现 DS 时签发短期 JWT(auth.DSCallbackClaims,
//     绑定 match_id / pod),经 GameServer annotation 或 env 下发,DS 拿不到签名密钥
//   - DS 回调时带 `authorization: Bearer <token>`
//   - Envoy 在 :8444 路由上打 `x-pandora-ds-gateway: 1` 标记头(入站同名头先剥离防伪造),
//     后端据此区分「DS 面进来的回调」与「集群内东西向内部调用」(login/matchmaker 等
//     内部服务直连业务端口调 SetLocation,不带令牌、不经网关,不受本守卫影响)
//   - 被回调服务在 handler 顶部调 Guard.Check(ctx, scope) 做验签 + 范围绑定
//
// 灰度(config.DSAuthConf.Mode,CLAUDE.md §14 开关默认关、开启分支完整):
//   - off        → 直接放行(默认;UE DS 未携带令牌前必须 off)
//   - permissive → 校验失败只记 warn 日志仍放行(观察期,确认 DS 侧全量带上令牌)
//   - enforce    → 经 DS 面网关的请求必须带有效且范围匹配的令牌,否则拒绝
package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/go-kratos/kratos/v2/transport"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
)

// VerifiedCredential 是从**验签通过且范围匹配**的 Model B hub 回调令牌里抽出的凭据身份
// (decision-revisit-ds-callback-auth §7)。仅当令牌携带 Model B 身份(instance_uid+
// protocol_epoch 非空)时非 nil;legacy 令牌(只有 gen / 无 uid)返回 nil,由调用方回退到
// 旧代际门。TokenSHA256 由守卫对原始令牌串计算,供授权记录 promote 时做完整性绑定。
type VerifiedCredential struct {
	DSType        auth.DSType
	MatchID       uint64
	Pod           string
	InstanceUID   string
	ProtocolEpoch uint32
	Gen           uint64
	JTI           string
	ExpMs         int64
	TokenSHA256   string
	Kid           string
	WriterEpoch   uint32
}

// MetadataKeyDSGateway 是 Envoy DS 面(:8444)给上游请求打的来源标记头。
// Envoy 在 :8443/:8444 入站先无条件剥离同名头(防客户端/DS 伪造),再由 :8444 路由重新写入。
const MetadataKeyDSGateway = "x-pandora-ds-gateway"

// authorizationHeader 是 DS 携带 Bearer 令牌的标准头。
const authorizationHeader = "authorization"

// DSAuthMode 是 DS 回调令牌校验模式。
type DSAuthMode int

const (
	// DSAuthOff 不校验(默认)。
	DSAuthOff DSAuthMode = iota
	// DSAuthPermissive 校验但失败只记日志(灰度观察)。
	DSAuthPermissive
	// DSAuthEnforce 经 DS 面网关的回调必须带有效且范围匹配的令牌。
	DSAuthEnforce
)

// String 实现 fmt.Stringer(日志用)。
func (m DSAuthMode) String() string {
	switch m {
	case DSAuthPermissive:
		return "permissive"
	case DSAuthEnforce:
		return "enforce"
	default:
		return "off"
	}
}

// ParseDSAuthMode 解析 config.DSAuthConf.Mode;空串等价 "off",非法值报错(main fatal)。
func ParseDSAuthMode(s string) (DSAuthMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "off":
		return DSAuthOff, nil
	case "permissive":
		return DSAuthPermissive, nil
	case "enforce":
		return DSAuthEnforce, nil
	default:
		return DSAuthOff, fmt.Errorf("ds_auth.mode invalid: %q (want off|permissive|enforce)", s)
	}
}

// DSScope 是一次回调允许的授权范围(handler 按请求参数填)。
type DSScope struct {
	// Type 期望的 DS 类型(hub/battle);空不校验(不建议)。
	Type auth.DSType
	// MatchID 非 0 时要求令牌 match_id 与之一致(battle 令牌:ReportResult / Heartbeat / GM)。
	MatchID uint64
	// Pod 非空时要求令牌 sub(pod 名)与之一致(hub 令牌:Heartbeat / SetLocation / ReportDisconnect)。
	Pod string
	// DenyDS 为 true 表示「这种调用形态根本不该来自 DS」(例:SetLocation state != HUB,
	// MATCHING/BATTLE 位置只允许 matchmaker/ds_allocator 内部写)。命中时:经网关的请求
	// 或带 DS 令牌的请求一律按越权处理(enforce 拒绝 / permissive 告警)。
	DenyDS bool
	// RequireToken 为 true 表示「本回调只可能来自 DS,没有合法的东西向内部无令牌调用者」。
	// 纯 DS 回调:hub/battle Heartbeat、ReportResult、GM Poll·Ack、SetLocation(state=HUB)、
	// ReportDisconnect(全仓确认无任何内部 Go 服务写 HUB 或调 ReportDisconnect)。命中时即使
	// 请求不带网关标记头,只要没有有效令牌也一律拒绝(enforce)/ 告警(permissive)——堵住
	// 「被攻破的业务 Pod 绕过 Envoy 直连业务端口、无标记无令牌却被当内部东西向信任」的旁路(审核 P1)。
	// SetLocation(state!=HUB:MATCHING/BATTLE/LOGIN_PENDING)确有合法内部无令牌调用者
	// (matchmaker/ds_allocator/login),走 DenyDS 而非本位。
	RequireToken bool
}

// DSCallbackGuard 校验 DS 回调令牌 + 范围绑定。nil Guard 等价 mode=off(未配置服务零改动)。
type DSCallbackGuard struct {
	verifier DSCallbackTokenVerifier
	mode     DSAuthMode
}

// DSCallbackTokenVerifier 是 Guard 唯一可见的验签方法集；玩家 Session/DSTicket 方法
// 不进入本信任域。生产 wiring 使用 *auth.DSCallbackVerifier。
type DSCallbackTokenVerifier interface {
	VerifyDSCallback(token string) (*auth.DSCallbackClaims, error)
}

// NewDSCallbackGuard 构造守卫。mode != off 时 verifier 必须非 nil。
func NewDSCallbackGuard(verifier DSCallbackTokenVerifier, mode DSAuthMode) (*DSCallbackGuard, error) {
	if mode != DSAuthOff && verifier == nil {
		return nil, fmt.Errorf("ds_auth: mode=%s requires verifier (ds_auth.secret)", mode)
	}
	return &DSCallbackGuard{verifier: verifier, mode: mode}, nil
}

// Mode 返回当前校验模式(启动日志用)。
func (g *DSCallbackGuard) Mode() DSAuthMode {
	if g == nil {
		return DSAuthOff
	}
	return g.mode
}

// Check 在 DS 回调 handler 顶部调用;返回非 nil 时 handler 应把它翻译成业务错误码返回。
//
// 判定表(mode=enforce;permissive 把「拒绝」降级为 warn 日志放行):
//
//	经 :8444 网关(带标记头) + 无/无效令牌      → ErrUnauthorized
//	经 :8444 网关            + 令牌范围不匹配    → ErrPermissionDeny
//	内部直连(无标记头)     + 无令牌            → 放行(东西向内部调用不受影响)
//	内部直连(无标记头)     + 无令牌 + RequireToken → ErrUnauthorized(纯 DS 回调无合法无令牌调用者)
//	任意来源                 + 带令牌但范围不匹配 → ErrPermissionDeny(带 DS 令牌即视为 DS)
//	scope.DenyDS             + 经网关或带令牌     → ErrPermissionDeny
func (g *DSCallbackGuard) Check(ctx context.Context, scope DSScope) error {
	_, err := g.CheckWithClaims(ctx, scope)
	return err
}

// CheckWithClaims 同 Check,但额外返回**真正验签通过且范围匹配**的令牌 claims。
// claims 非 nil 仅当:请求带令牌、验签成功、且 scope 全部匹配。mode=off / nil guard /
// 无令牌放行 / permissive 降级放行时 claims 为 nil —— 调用方据此区分「鉴权过的事实」与
// 「未验证的放行」,禁止把 nil claims 当作已证明(hub 心跳 warming→ready 代际绑定用,二审 #3/#4)。
func (g *DSCallbackGuard) CheckWithClaims(ctx context.Context, scope DSScope) (*auth.DSCallbackClaims, error) {
	if g == nil || g.mode == DSAuthOff {
		return nil, nil
	}
	viaGateway := dsGatewayMarked(ctx)
	token := bearerToken(ctx)

	// 该调用形态不允许来自 DS:经网关进来或带 DS 令牌都按越权处理。
	if scope.DenyDS {
		if viaGateway || token != "" {
			return nil, g.reject(ctx, errcode.ErrPermissionDeny, viaGateway,
				"call shape not allowed from ds gateway")
		}
		return nil, nil
	}

	if token == "" {
		if !viaGateway {
			// 纯 DS 回调(RequireToken):没有合法的东西向内部无令牌调用者,无令牌即拒,
			// 堵住「绕过 Envoy 直连业务端口、无标记无令牌被当内部信任」的旁路。
			if scope.RequireToken {
				return nil, g.reject(ctx, errcode.ErrUnauthorized, viaGateway,
					"missing bearer token (ds-only callback rejects unauthenticated east-west call)")
			}
			return nil, nil // 集群内东西向内部调用(login/matchmaker 等),不带令牌、不经网关
		}
		return nil, g.reject(ctx, errcode.ErrUnauthorized, viaGateway, "missing bearer token")
	}

	claims, err := g.verifier.VerifyDSCallback(token)
	if err != nil {
		return nil, g.reject(ctx, errcode.ErrUnauthorized, viaGateway, "token invalid: %v", err)
	}
	if scope.Type != "" && claims.DSType != string(scope.Type) {
		return nil, g.reject(ctx, errcode.ErrPermissionDeny, viaGateway,
			"ds_type mismatch: token=%s want=%s", claims.DSType, scope.Type)
	}
	if scope.MatchID != 0 && claims.MatchID != scope.MatchID {
		return nil, g.reject(ctx, errcode.ErrPermissionDeny, viaGateway,
			"match_id mismatch: token=%d req=%d", claims.MatchID, scope.MatchID)
	}
	if scope.Pod != "" && claims.Pod() != scope.Pod {
		return nil, g.reject(ctx, errcode.ErrPermissionDeny, viaGateway,
			"pod mismatch: token=%q req=%q", claims.Pod(), scope.Pod)
	}
	return claims, nil
}

// CheckCredential 校验任意 Hub/Battle DS 回调令牌并抽出完整 Model B 凭据。
//
// VerifyDSTicket 这类同时允许 Hub/Battle DS 调用的方法在读取玩家票据前还不知道
// 调用方类型，因此不能预先把 scope.Type/MatchID 写死。调用方应至少把请求中的 Pod
// 与 RequireToken=true 传入；本方法仍由 VerifyDSCallback 严格校验 ds_type，随后只在
// 完整的 (pod,uid,epoch,gen,jti,exp,kid,writer) 都存在时返回 credential。
// legacy/不完整令牌返回 nil credential，由 authority_mode=redis 的调用方 fail-closed。
func (g *DSCallbackGuard) CheckCredential(ctx context.Context, scope DSScope) (*auth.DSCallbackClaims, *VerifiedCredential, error) {
	claims, err := g.CheckWithClaims(ctx, scope)
	if err != nil || claims == nil {
		return claims, nil, err
	}
	if claims.Pod() == "" || claims.UID() == "" || claims.Epoch() == 0 || claims.Gen() == 0 ||
		claims.JTI() == "" || claims.Kid() == "" || claims.WriterEpoch() == 0 || claims.ExpiresAt == nil {
		return claims, nil, nil
	}
	return claims, verifiedCredentialFromClaims(ctx, claims), nil
}

// CheckHubCredential 同 CheckWithClaims,但额外抽出 Model B hub 凭据身份(§7)。
//
// 返回的 *VerifiedCredential 非 nil 仅当:CheckWithClaims 返回了已验签且范围匹配的 claims,
// 且该 claims 携带 Model B 身份(instance_uid 非空 + protocol_epoch 非 0)。legacy 令牌
// (只有 ds_gen、无 uid/epoch)返回 (claims, nil, nil),调用方回退旧代际门;mode=off /
// 无令牌放行 / permissive 降级放行时返回 (nil, nil, nil)。
// TokenSHA256 对原始 Bearer 令牌串计算,供授权记录 promote 完整性绑定。
func (g *DSCallbackGuard) CheckHubCredential(ctx context.Context, scope DSScope) (*auth.DSCallbackClaims, *VerifiedCredential, error) {
	return g.CheckCredential(ctx, scope)
}

// CheckBattleCredential 校验 battle 回调的 match+pod scope，并抽出完整 Model B 凭据。
// legacy battle token（无 pod/UID/epoch/gen/jti/kid/writer）返回 nil credential，由 Battle
// authority_mode=redis 的 handler 在任何副作用前拒绝；只有 Heartbeat 可拿 pending 激活。
func (g *DSCallbackGuard) CheckBattleCredential(ctx context.Context, scope DSScope) (*auth.DSCallbackClaims, *VerifiedCredential, error) {
	claims, credential, err := g.CheckCredential(ctx, scope)
	if err != nil || claims == nil {
		return claims, nil, err
	}
	if claims.DSType != string(auth.DSTypeBattle) || claims.MatchID == 0 || claims.Pod() == "" ||
		claims.UID() == "" || claims.Epoch() == 0 || claims.Gen() == 0 || claims.JTI() == "" ||
		claims.Kid() == "" || claims.WriterEpoch() == 0 || claims.ExpiresAt == nil {
		return claims, nil, nil
	}
	return claims, credential, nil
}

func verifiedCredentialFromClaims(ctx context.Context, claims *auth.DSCallbackClaims) *VerifiedCredential {
	var expMs int64
	if exp := claims.ExpiresAt; exp != nil {
		expMs = exp.Time.UnixMilli()
	}
	cred := &VerifiedCredential{
		DSType:        auth.DSType(claims.DSType),
		MatchID:       claims.MatchID,
		Pod:           claims.Pod(),
		InstanceUID:   claims.UID(),
		ProtocolEpoch: claims.Epoch(),
		Gen:           claims.Gen(),
		JTI:           claims.JTI(),
		ExpMs:         expMs,
		Kid:           claims.Kid(),
		WriterEpoch:   claims.WriterEpoch(),
	}
	if raw := bearerToken(ctx); raw != "" {
		sum := sha256.Sum256([]byte(raw))
		cred.TokenSHA256 = hex.EncodeToString(sum[:])
	}
	return cred
}

// ── 装配便利函数(各服务 main 一行接入)──────────────────────────────────────────

// NewDSCallbackSignerFromConf 按 ds_auth 配置构造 DS 回调令牌签发器。
// Secret 未配 → (nil, nil) 表示本服务不签发(调用方跳过注入)。
func NewDSCallbackSignerFromConf(cfg config.DSAuthConf) (*auth.DSCallbackSigner, error) {
	if cfg.Secret == "" {
		return nil, nil
	}
	return auth.NewDSCallbackSigner(auth.Config{
		Issuer:   cfg.Issuer,
		Audience: cfg.Audience,
		Secret:   []byte(cfg.Secret),
	})
}

// NewDSCallbackVerifierFromConf 按 ds_auth 配置构造 DS 回调令牌验签器。
// Secret 未配 → (nil, nil)(调用方据此跳过验签,如 agones 续期只看外置 exp)。
// 供签发侧(hub_allocator)在续期判据里实测 annotation 令牌确实验签通过,挡住空 / 损坏 /
// 旧密钥(轮换后)签发的令牌被“exp 未近 + 有 token 字段”误判为可用(审核 P1)。
func NewDSCallbackVerifierFromConf(cfg config.DSAuthConf) (*auth.DSCallbackVerifier, error) {
	if cfg.Secret == "" {
		return nil, nil
	}
	additional, err := dsAuthAdditionalSecretBytes(cfg)
	if err != nil {
		return nil, err
	}
	return auth.NewDSCallbackVerifier(auth.Config{
		Issuer:            cfg.Issuer,
		Audience:          cfg.Audience,
		Secret:            []byte(cfg.Secret),
		AdditionalSecrets: additional,
	})
}

// NewDSCallbackGuardFromConf 按 ds_auth 配置构造校验守卫。
//   - mode=off(含空)→ (nil, nil),handler 侧 nil guard 直接放行
//   - mode=permissive/enforce 但 Secret 未配 → error(配置矛盾,启动即 fatal 而非静默不校验)
func NewDSCallbackGuardFromConf(cfg config.DSAuthConf) (*DSCallbackGuard, error) {
	mode, err := ParseDSAuthMode(cfg.Mode)
	if err != nil {
		return nil, err
	}
	if mode == DSAuthOff {
		return nil, nil
	}
	if cfg.Secret == "" {
		return nil, fmt.Errorf("ds_auth: mode=%s requires ds_auth.secret", mode)
	}
	additional, err := dsAuthAdditionalSecretBytes(cfg)
	if err != nil {
		return nil, err
	}
	verifier, err := auth.NewDSCallbackVerifier(auth.Config{
		Issuer:            cfg.Issuer,
		Audience:          cfg.Audience,
		Secret:            []byte(cfg.Secret),
		AdditionalSecrets: additional,
	})
	if err != nil {
		return nil, err
	}
	return NewDSCallbackGuard(verifier, mode)
}

// dsAuthAdditionalSecretBytes 把配置里的额外校验密钥(字符串)转成 [][]byte。
// 支持 DS 回调令牌不停服轮换(审核 P1 #3):校验侧接受主密钥 + 这些额外密钥。
// 空串条目是配置事故(轮换清单少写了一把却留了占位):静默过滤会让运维以为旧密钥
// 仍被接受、实则轮换断档 → 启动即报错(二审 #8,fail-closed)。
func dsAuthAdditionalSecretBytes(cfg config.DSAuthConf) ([][]byte, error) {
	if len(cfg.AdditionalSecrets) == 0 {
		return nil, nil
	}
	out := make([][]byte, 0, len(cfg.AdditionalSecrets))
	for i, s := range cfg.AdditionalSecrets {
		if s == "" {
			return nil, fmt.Errorf("ds_auth: additional_secrets[%d] is empty (rotation misconfig; remove the entry or fill the key)", i)
		}
		out = append(out, []byte(s))
	}
	return out, nil
}

// reject 按模式产出拒绝:enforce 返回错误;permissive 记 warn 放行(返回 nil)。
func (g *DSCallbackGuard) reject(ctx context.Context, code errcode.Code, viaGateway bool, format string, args ...any) error {
	if g.mode == DSAuthEnforce {
		// enforce 拒绝同样必须落盘(§11.3 R2):这些码(ErrUnauthorized/ErrInvalidArg)非
		// IsServerFault,access log 只记 rpc_ok=DEBUG;各服务调用点并不逐点补日志,这里是
		// 所有 DS 回调鉴权拒绝的唯一必经收口——DS 凭据轮换断档时,一台 DS 上全部玩家的
		// 回调会成批被拒,没有这条就只能靠客户端表象反推。拒绝是低频异常路径,不构成噪音。
		plog.With(ctx).Warnw("msg", "ds_callback_auth_rejected",
			"reason", fmt.Sprintf(format, args...), "via_gateway", viaGateway, "code", int32(code))
		return errcode.New(code, "ds callback auth: "+format, args...)
	}
	plog.With(ctx).Warnw("msg", "ds_callback_auth_permissive_reject",
		"reason", fmt.Sprintf(format, args...), "via_gateway", viaGateway)
	return nil
}

// dsGatewayMarked 判断请求是否经 DS 面网关进入(Envoy :8444 路由注入的标记头)。
func dsGatewayMarked(ctx context.Context) bool {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return false
	}
	return tr.RequestHeader().Get(MetadataKeyDSGateway) != ""
}

// bearerToken 从 authorization 头取 Bearer 令牌;无 / 非 Bearer 返回空串。
func bearerToken(ctx context.Context) string {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return ""
	}
	v := strings.TrimSpace(tr.RequestHeader().Get(authorizationHeader))
	if v == "" {
		return ""
	}
	const prefix = "bearer "
	if len(v) > len(prefix) && strings.EqualFold(v[:len(prefix)], prefix) {
		return strings.TrimSpace(v[len(prefix):])
	}
	return ""
}

// DSBearerToken 返回当前已携带的原始 Bearer token，供一个已经完成校验的服务把同一凭据
// 转发给下游继续执行 active-state 校验。调用方不得记录、持久化或放进错误文本。
func DSBearerToken(ctx context.Context) string {
	return bearerToken(ctx)
}
