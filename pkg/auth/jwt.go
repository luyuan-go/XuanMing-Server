// Package auth 提供 Pandora 统一的 JWT 签发 / 校验工具。
//
// W3 ① 落地(2026-06-05):
//   - login 服务签:
//   - SessionToken:玩家登录后的会话凭证(sub=player_id, exp=24h)
//   - DSTicket:玩家进入 hub / battle DS 前的短期票据(exp=5min)
//   - Envoy 边缘网关用 jwt_authn filter 校验 SessionToken,把 sub claim 提到
//     `x-pandora-player-id` 头(给业务服 middleware 用)
//   - 业务服(push / 后续 13 服)收到请求时 player_id 已在 header 里,
//     不需要再解 JWT;但 DSTicket 还得在 login.VerifyDSTicket 里二次校验(防重放 jti)
//
// 选型:
//   - 算法 HS256(对称 HMAC):dev 期最简单;Envoy jwt_authn 用 `local_jwks` inline
//     一份 `kty=oct` 的 JWKS 即可。**生产期切 RS256**(login 私钥签 / Envoy 公钥验,
//     防 Envoy 被攻破后能签任意 token)。
//   - 库 github.com/golang-jwt/jwt/v5:维护活跃、API 稳。
//
// 不变量(CLAUDE.md §9):
//   - 不变量 §3:DS 票据短时效(本包 DSTicketTTL 默认 5min)
//   - 不变量 §5:proto 字段编号永不复用(本包 claim key 也照此原则,新增不删旧)
//   - jti 必须每次唯一(uuid v4),防重放靠 redis 黑名单(W3 ② 接入,见 TODO)
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// DSType 区分票据签的是哪种 DS。
type DSType string

const (
	DSTypeHub    DSType = "hub"
	DSTypeBattle DSType = "battle"
)

// SessionClaims 是 SessionToken 的载荷。
//
// 标准 RegisteredClaims:iss / sub / aud / exp / iat / jti
//   - sub:player_id 的十进制字符串(JWT 规范 sub 是 string)
//   - aud:固定 "pandora-client"(Envoy jwt_authn 校验)
//   - iss:固定 "pandora-login"
//   - exp:发行时刻 + SessionTTL(默认 24h)
//   - jti:uuid v4,W3 ② redis 加黑名单可吊销
type SessionClaims struct {
	jwt.RegisteredClaims
}

// AccountClaims 是**账号态 token** 的载荷(两步登录,2026-08-18)。
//
// 与 SessionClaims 的分工:
//   - AccountClaims.sub = account_id,只能用来「列出本账号的角色」与「选一个角色进入」;
//   - SessionClaims.sub = player_id(角色实体),才是玩家面一切 RPC 的身份。
//
// 两者用不同 aud(见 Config.AccountAudience 注释)彻底隔离,互相不可通过对方的校验。
// 账号态刻意**不带**角色信息:选角之前服务端还没有为这次会话绑定任何角色,带上去等于
// 让客户端自报,违反 §9.6「不信客户端自报身份」。
type AccountClaims struct {
	jwt.RegisteredClaims
}

// AccountID 把 sub 字符串解成 uint64。失败返回 0。
func (a *AccountClaims) AccountID() uint64 {
	if a.Subject == "" {
		return 0
	}
	id, err := strconv.ParseUint(a.Subject, 10, 64)
	if err != nil {
		return 0
	}
	return id
}

// PlayerID 把 sub 字符串解成 uint64。失败返回 0。
func (s *SessionClaims) PlayerID() uint64 {
	if s.Subject == "" {
		return 0
	}
	id, err := strconv.ParseUint(s.Subject, 10, 64)
	if err != nil {
		return 0
	}
	return id
}

// DSTicketClaims 是 DSTicket 的载荷(短时效,5min)。
//
// 自定义 claim:
//   - ds_type:"hub" / "battle"
//   - match_id:battle DS 才有(hub 为 0)
//   - region_id / cell_id:玩家确定性路由落点(docs/design/scale-cellular-20m.md §3.3)。
//     把 DS 票据绑定到 Region+Cell,防跨单元串号(stale / 伪造票据把玩家从 A 单元的 DS
//     接进 B 单元)。omitempty:单 Cell / dev(0)时不序列化该 claim,与历史票据完全兼容。
//     uint32 拓扑维度(非 snowflake 业务 ID,CLAUDE.md §9.12)。
type DSTicketClaims struct {
	jwt.RegisteredClaims
	DSType   string `json:"ds_type"`
	MatchID  uint64 `json:"match_id,omitempty"`
	RegionID uint32 `json:"region_id,omitempty"`
	CellID   uint32 `json:"cell_id,omitempty"`
	// RoleID:玩家已选角色配置 ID(CfgRole.Id,选角权威化 2026-07-08)。hub 票据携带,
	// DS 验签后直接用它 spawn 角色,不再信任客户端 URL ?role= 自报。
	// omitempty:0(未选角 / battle 票走 roster)时不序列化,与历史票据兼容。
	// uint32 配置表 ID(非 snowflake 业务 ID,CLAUDE.md §9.12)。
	RoleID uint32 `json:"role_id,omitempty"`
	// 以下字段把 hub 入场票绑定到「当前玩家归属 + 当前 Hub DS active 凭据」。
	// 全部使用 omitempty 保持旧票据字节/解析兼容；Model B enforce 由上层要求六项必须完整。
	DSPodName       string `json:"ds_pod,omitempty"`
	DSInstanceUID   string `json:"ds_uid,omitempty"`
	DSProtocolEpoch uint32 `json:"ds_epoch,omitempty"`
	DSCredentialGen uint64 `json:"ds_gen,omitempty"`
	DSCredentialJTI string `json:"ds_credential_jti,omitempty"`
	HubAssignmentID string `json:"hub_assignment_id,omitempty"`
	DSWriterEpoch   uint32 `json:"ds_writer_epoch,omitempty"`
	// SourceMatchID:Battle→Hub 回流 fence(与 DSTicketClaimsV2.SourceMatchID 同义,
	// 2026-07-21)。仅 hub 票可带(>0 表示从该终局对局返回大厅);battle 票必须 0。
	SourceMatchID uint64 `json:"source_match_id,omitempty"`
}

// DSTicketBinding 是 hub DSTicket 的实例/归属绑定。零值表示旧兼容票据；非零时六项必须完整，
// 身份为 assignment_id + (pod, instance_uid, protocol_epoch, gen, credential_jti)。
type DSTicketBinding struct {
	DSPodName       string
	DSInstanceUID   string
	ProtocolEpoch   uint32
	CredentialGen   uint64
	CredentialJTI   string
	HubAssignmentID string
	WriterEpoch     uint32
}

// PlayerID 把 sub 字符串解成 uint64。失败返回 0。
func (t *DSTicketClaims) PlayerID() uint64 {
	if t.Subject == "" {
		return 0
	}
	id, err := strconv.ParseUint(t.Subject, 10, 64)
	if err != nil {
		return 0
	}
	return id
}

// ── DS 回调服务令牌(审核 P1 #1,2026-07-10)────────────────────────────────────
//
// 与 DSTicket(玩家→DS 的入场票)方向相反:DSCallbackToken 是「DS→后端」的服务身份令牌,
// 证明回调方(Heartbeat / ReportResult / SetLocation / PollCommands …)确实是后端刚分配 /
// 发现的那个 DS 实例,而不是集群内的伪造调用者。
//
//   - 签发方:ds_allocator(战斗 DS,分配时签,绑 match_id)/ hub_allocator(大厅 DS,
//     发现时签,绑 pod 名,临期续签)
//   - 下发通道:Agones GameServer annotation `pandora.dev/ds-token`(分配 metadata /
//     发现后 PATCH)或 local 模式 PANDORA_DS_TOKEN env——DS 全程拿不到签名密钥
//   - 携带方式:DS 回调时 `authorization: Bearer <token>`
//   - 校验方:pkg/middleware.DSCallbackGuard(四个被回调服务按 ds_auth.mode 灰度)
//
// aud 与玩家令牌严格分域(默认 "pandora-ds" vs "pandora-client"),互不可用。

// DSCallbackClaims 是 DS 回调服务令牌的载荷。
//
// 自定义 claim:
//   - ds_type:"hub" / "battle"
//   - match_id:battle 令牌必填(授权范围 = 本场对局);hub 为 0 不序列化
//
// sub:hub 令牌 = GameServer pod 名(授权范围 = 本实例);battle 令牌签发时尚不知道
// Agones 会选中哪个 GameServer(分配 metadata 原子下发),故 sub 留空、以 match_id 为准
// (match↔pod 的绑定由 ds_allocator 心跳镜像的 pod_mismatch 校验闭环)。
type DSCallbackClaims struct {
	jwt.RegisteredClaims
	DSType  string `json:"ds_type"`
	MatchID uint64 `json:"match_id,omitempty"`
	// DSGen 是 hub 回调令牌的「代际」(Redis INCR 权威、独立、单调;0=未启用/battle 令牌)。
	// hub_allocator 每次重签 Hub 令牌领取一个严格递增的 gen 签进来,DS 心跳原样回显,
	// 服务端据此精确相等比较判定令牌是否当前代际(替代秒级 exp 代际,消除同秒重签碰撞;审核 P1-6)。
	DSGen uint64 `json:"ds_gen,omitempty"`
	// DSInstanceUID / DSProtocolEpoch:Model B 凭据身份绑定(decision-revisit-ds-callback-auth §7)。
	// 凭据身份 = (instance_uid, protocol_epoch, gen, jti) 四元组:gen 单独不安全(计数器 TTL
	// 复位可致 gen 复用),必须联合 DS 实例身份(Agones GameServer uid)与协议纪元才能唯一锚定。
	// Redis Model B 生产路径以及机械隔离的 local-off-v1 均必须非空；legacy 令牌保持空
	// (omitempty)只用于旧二进制迁移读取，新 UE 不再把它当可用凭据。
	DSInstanceUID   string `json:"ds_uid,omitempty"`
	DSProtocolEpoch uint32 `json:"ds_epoch,omitempty"`
	// DSWriterEpoch 是二进制授权协议能力纪元；与 DSProtocolEpoch(实例轮次)严格分离。
	DSWriterEpoch uint32 `json:"ds_writer_epoch,omitempty"`
	// DSKid 是签发密钥指纹的签名内副本；active-state checker 不依赖未签名的外部 annotation。
	DSKid string `json:"ds_kid,omitempty"`
}

// Pod 返回令牌绑定的 GameServer pod 名(hub 令牌;battle 令牌为空)。
func (c *DSCallbackClaims) Pod() string { return c.Subject }

// Gen 返回令牌代际(hub 令牌;battle 令牌 / 未启用为 0)。
func (c *DSCallbackClaims) Gen() uint64 { return c.DSGen }

// UID 返回凭据绑定的 DS 实例身份(Agones GameServer uid;Model B 未启用为空)。
func (c *DSCallbackClaims) UID() string { return c.DSInstanceUID }

// Epoch 返回凭据协议纪元(Model B 未启用为 0)。
func (c *DSCallbackClaims) Epoch() uint32 { return c.DSProtocolEpoch }

// WriterEpoch 返回签发该凭据的授权协议能力纪元。
func (c *DSCallbackClaims) WriterEpoch() uint32 { return c.DSWriterEpoch }

// Kid 返回签发密钥指纹的签名 claim。
func (c *DSCallbackClaims) Kid() string { return c.DSKid }

// JTI 返回令牌唯一 id(RegisteredClaims.ID)。
func (c *DSCallbackClaims) JTI() string { return c.ID }

// DS 回调令牌的默认 iss / aud(config.DSAuthConf.Defaults 同步这两个值)。
const (
	DSCallbackIssuer   = "pandora-ds-control"
	DSCallbackAudience = "pandora-ds"
	// DSAuthWriterEpochV2 是本二进制实现的 Model B writer capability。
	DSAuthWriterEpochV2 uint32 = 2
)

// Config 是 JWT signer / verifier 公共配置。
//
// Secret 必须 ≥ 32 字节(HS256 推荐安全长度);生产期换 RS256 时本字段废弃,
// 改为 PrivateKeyPEM / PublicKeyPEM。
type Config struct {
	// Issuer 固定 "pandora-login"(JWT iss 字段)。
	Issuer string

	// Audience 固定 "pandora-client"(JWT aud 字段,Envoy jwt_authn 校验)。
	Audience string

	// AccountAudience 是**账号态 token** 的 aud,默认 "pandora-account"。
	//
	// 为什么必须与 Audience 不同(两步登录 2026-08-18 的安全根基):
	// Envoy 的 jwt_authn provider 把 sub claim 通过 claim_to_headers 注入
	// x-pandora-player-id,全后端都拿这个头当玩家身份。账号态 token 的 sub 是
	// **account_id 而不是 player_id**;两者若共用同一个 aud,一张账号 token 就能通过
	// pandora_session provider,把 account_id 冒充成 player_id 打进 team / chat /
	// inventory 等一切玩家面 RPC —— 这是直接的越权。
	//
	// 靠不同 aud 隔离,是因为 aud 校验发生在**签名校验的同一层**(Envoy 侧由 provider
	// 的 audiences 强制,Go 侧由 jwt.WithAudience 强制),攻击者拿不到密钥就伪造不出。
	// 换成「加一个 typ claim 再由业务层判别」是更弱的方案:Envoy 在业务层之前就已经
	// 把头注入好了,漏判一处即越权。
	AccountAudience string

	// Secret HS256 共享密钥;dev 期 login 服务跟 Envoy 各持一份(同一字符串)。
	Secret []byte

	// AdditionalSecrets 是**仅用于校验**的额外可接受密钥(不用于签发),支持不停服密钥轮换
	// (审核 P1 #3)。签发始终用 Secret(主密钥);校验时按 token 头 kid 路由到匹配密钥,
	// 无 / 未知 kid 时依次尝试 [Secret, AdditionalSecrets...]。轮换三段式:
	//   ① 各服务先把新密钥 K2 加进 additional(仍用 K1 签)→ 全量接受 K1/K2;
	//   ② 主密钥翻成 K2、additional 放 K1 → 用 K2 签,仍接受 K1;
	//   ③ 清空 additional 只留 K2。
	// 三步都是滚动更新,新旧副本共存期两把密钥都被接受,无 401 断档(CLAUDE.md §9 不变量 16)。
	// 默认 nil:单密钥,行为与历史完全一致。
	AdditionalSecrets [][]byte

	// SessionTTL SessionToken 有效期,默认 24h。
	SessionTTL time.Duration

	// AccountTTL 账号态 token 有效期,默认 10min。
	//
	// 刻意远短于 SessionTTL:它只需要覆盖「登录成功 → 看角色列表 → 选一个进去」这段
	// 交互,不是常驻凭据。过期了重新登录即可,代价只有一次输密码;而它一旦泄漏,
	// 拿到的是**整个账号下所有角色**的进入权(比单个角色的 session 影响面更大),
	// 所以窗口必须小。
	AccountTTL time.Duration

	// DSTicketTTL DSTicket 有效期,默认 5min(不变量 §3)。
	DSTicketTTL time.Duration

	// NowFn 可注入(测试用),默认 time.Now。
	NowFn func() time.Time
}

// Defaults 把零值填默认。
func (c *Config) Defaults() {
	if c.Issuer == "" {
		c.Issuer = "pandora-login"
	}
	if c.Audience == "" {
		c.Audience = "pandora-client"
	}
	if c.AccountAudience == "" {
		c.AccountAudience = "pandora-account"
	}
	if c.SessionTTL == 0 {
		c.SessionTTL = 24 * time.Hour
	}
	if c.AccountTTL == 0 {
		c.AccountTTL = 10 * time.Minute
	}
	if c.DSTicketTTL == 0 {
		c.DSTicketTTL = 5 * time.Minute
	}
	if c.NowFn == nil {
		c.NowFn = time.Now
	}
}

// Validate 拒绝弱配置。
//
// HS256 推荐 secret 长度 ≥ 算法输出长度 (256 bit = 32 byte),RFC 7518 §3.2 明说
// “A key of the same size as the hash output ... is considered sufficient”。低于 32 字节
// 会被 golang-jwt/jwt/v5 未来版本拒签 (已结论为 CVE 倒退)。
func (c *Config) Validate() error {
	if len(c.Secret) < 32 {
		return fmt.Errorf("auth.Config: Secret too short (got %d bytes, need >=32 for HS256)", len(c.Secret))
	}
	// JWT NumericDate 以秒为粒度；小于 1s 的 TTL 可能在签发时就被截断为过期。
	// 负值不会被 Defaults 修正，也必须在启动期拒绝，避免 Login 写入立即失效的会话。
	if c.SessionTTL < time.Second {
		return fmt.Errorf("auth.Config: SessionTTL must be >=1s (got %s)", c.SessionTTL)
	}
	if c.DSTicketTTL < time.Second {
		return fmt.Errorf("auth.Config: DSTicketTTL must be >=1s (got %s)", c.DSTicketTTL)
	}
	if c.AccountTTL < time.Second {
		return fmt.Errorf("auth.Config: AccountTTL must be >=1s (got %s)", c.AccountTTL)
	}
	// 账号态与玩家态共用一个 aud = 越权。账号 token 的 sub 是 account_id,一旦能通过
	// Envoy 的 pandora_session provider,account_id 就会被 claim_to_headers 注入
	// x-pandora-player-id,冒充成玩家身份打进全部玩家面 RPC。这是配置层唯一能机械
	// 拦住的一环,故启动期硬拒,不留「配错了只是告警」的余地。
	if c.AccountAudience == c.Audience {
		return fmt.Errorf("auth.Config: AccountAudience must differ from Audience (both %q); "+
			"共用 aud 会让账号态 token 通过玩家态校验,account_id 被当作 player_id 注入", c.Audience)
	}
	for i, s := range c.AdditionalSecrets {
		if len(s) < 32 {
			return fmt.Errorf("auth.Config: AdditionalSecrets[%d] too short (got %d bytes, need >=32 for HS256)", i, len(s))
		}
		// 重复密钥必属误配(轮换三段式里 additional 只放「另一把」密钥):与主密钥同值 =
		// 轮换实际没生效;additional 内部重复 = 复制粘贴事故。静默接受会掩盖轮换失败 → 启动即拒。
		if SecretEqual(s, c.Secret) {
			return fmt.Errorf("auth.Config: AdditionalSecrets[%d] duplicates primary Secret (rotation misconfig)", i)
		}
		for j := 0; j < i; j++ {
			if SecretEqual(s, c.AdditionalSecrets[j]) {
				return fmt.Errorf("auth.Config: AdditionalSecrets[%d] duplicates AdditionalSecrets[%d]", i, j)
			}
		}
	}
	return nil
}

// verificationKeys 返回校验时依次尝试的密钥集合:主密钥在前,额外密钥在后。
func (c *Config) verificationKeys() [][]byte {
	keys := make([][]byte, 0, 1+len(c.AdditionalSecrets))
	keys = append(keys, c.Secret)
	keys = append(keys, c.AdditionalSecrets...)
	return keys
}

// keyFingerprint 取密钥的稳定短指纹(SHA256 前 8 字节的 hex),作为 token 头 kid。
// 不可逆、确定性:同一密钥恒得同一 kid,供轮换期按 kid 把 token 路由到对应校验密钥,
// 并让令牌自描述用了哪把密钥(审核 P1 #3:此前 token 无 kid)。
func keyFingerprint(secret []byte) string {
	sum := sha256.Sum256(secret)
	return hex.EncodeToString(sum[:8])
}

// Signer 签发 SessionToken / DSTicket。线程安全(无可变状态)。
type Signer struct {
	cfg Config
}

// Verifier 校验 SessionToken / DSTicket。线程安全。
type Verifier struct {
	cfg Config
}

// NewSigner 构造 Signer,Validate 失败 panic(只该在 main 启动期调用)。
func NewSigner(cfg Config) (*Signer, error) {
	cfg.Defaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Signer{cfg: cfg}, nil
}

// NewVerifier 构造 Verifier。
func NewVerifier(cfg Config) (*Verifier, error) {
	cfg.Defaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Verifier{cfg: cfg}, nil
}

// SessionTTL 暴露 SessionTTL 给调用方(login.biz 用来设置 redis session TTL,
// 保证 redis 过期跟 JWT exp 对齐)。
func (s *Signer) SessionTTL() time.Duration { return s.cfg.SessionTTL }

// DSTicketTTL 暴露 DSTicketTTL,用于 jti 防重放 TTL(verifier 校验通过后 SETNX)。
func (s *Signer) DSTicketTTL() time.Duration { return s.cfg.DSTicketTTL }

// DSTicketTTL 同上,verifier 侧也提供一个(调 jti SETNX 时常在 verify 处)。
func (v *Verifier) DSTicketTTL() time.Duration { return v.cfg.DSTicketTTL }

// SignSession 签发 SessionToken。jti 由调用方传(uuid v4)。
//
// 返回:JWT 字符串 / 过期时刻(unix ms,给客户端展示用)/ error。
func (s *Signer) SignSession(playerID uint64, jti string) (token string, expiresAtMs int64, err error) {
	if playerID == 0 {
		return "", 0, errors.New("auth.SignSession: playerID must be > 0")
	}
	if jti == "" {
		return "", 0, errors.New("auth.SignSession: jti must be non-empty")
	}
	now := s.cfg.NowFn()
	exp := now.Add(s.cfg.SessionTTL)
	claims := SessionClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.cfg.Issuer,
			Subject:   strconv.FormatUint(playerID, 10),
			Audience:  jwt.ClaimStrings{s.cfg.Audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
			ID:        jti,
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	str, err := t.SignedString(s.cfg.Secret)
	if err != nil {
		return "", 0, fmt.Errorf("auth.SignSession: %w", err)
	}
	return str, exp.UnixMilli(), nil
}

// AccountTTL 暴露 AccountTTL(login 用来对齐账号态 token 的过期展示)。
func (s *Signer) AccountTTL() time.Duration { return s.cfg.AccountTTL }

// SignAccount 签发**账号态 token**(两步登录第一步的产物)。
//
// sub = account_id;aud = AccountAudience(与玩家态严格分离)。jti 由调用方传(uuid v4)。
// 它只解锁 ListAccountRoles / EnterRole 两个入口,拿不到任何玩家面能力。
func (s *Signer) SignAccount(accountID uint64, jti string) (token string, expiresAtMs int64, err error) {
	if accountID == 0 {
		return "", 0, errors.New("auth.SignAccount: accountID must be > 0")
	}
	if jti == "" {
		return "", 0, errors.New("auth.SignAccount: jti must be non-empty")
	}
	now := s.cfg.NowFn()
	exp := now.Add(s.cfg.AccountTTL)
	claims := AccountClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.cfg.Issuer,
			Subject:   strconv.FormatUint(accountID, 10),
			Audience:  jwt.ClaimStrings{s.cfg.AccountAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
			ID:        jti,
		},
	}
	// 刻意**不设 kid 头**,与 SignSession 保持一致。
	// 账号态 token 跟 SessionToken 一样要经 Envoy jwt_authn 校验,而 envoy.yaml 的
	// local_jwks 里那把 key 的 kid 是 "pandora-dev"(一个固定字面量,不是密钥指纹)。
	// 一旦这里写进 keyFingerprint 指纹,Envoy 会按 kid 精确找 key、找不到就直接拒,
	// 账号态入口会全线 401。DS 回调 / Hub 凭据那几个 Sign* 设 kid 是因为它们只在 Go
	// 侧验签,不经 Envoy。
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	str, err := t.SignedString(s.cfg.Secret)
	if err != nil {
		return "", 0, fmt.Errorf("auth.SignAccount: %w", err)
	}
	return str, exp.UnixMilli(), nil
}

// SignDSTicket 签发 DS 票据。dsType / matchID 按 docs/design/proto-design.md DSTicket message。
//
// 不变量 §3:本方法默认 TTL=5min。
//
// 单 Cell / dev 语义(region/cell = 0):本方法等价于 SignDSTicketWithCell(...,0,0,...)。
// 多 Cell 部署请用 SignDSTicketWithCell 把玩家落点绑进票据(§3.3 防跨单元串号)。
func (s *Signer) SignDSTicket(playerID uint64, dsType DSType, matchID uint64, jti string) (token string, expiresAtMs int64, err error) {
	return s.SignDSTicketWithCell(playerID, dsType, matchID, 0, 0, jti)
}

// SignDSTicketWithCell 签发绑定 Region+Cell 的 DS 票据(docs/design/scale-cellular-20m.md §3.3)。
//
// regionID / cellID 是玩家的确定性路由落点(由调用方经 cellroute.Router 算出);单 Cell / dev
// 传 0(此时与 SignDSTicket 行为一致,claim 不序列化)。把落点签进票据后,DS 侧可校验
// "票据 Cell == 本 DS 所在 Cell",拒绝 stale / 伪造票据跨单元串号。
//
// 不变量 §3:默认 TTL=5min。
func (s *Signer) SignDSTicketWithCell(playerID uint64, dsType DSType, matchID uint64, regionID, cellID uint32, jti string) (token string, expiresAtMs int64, err error) {
	return s.SignDSTicketFull(playerID, dsType, matchID, regionID, cellID, 0, jti)
}

// SignDSTicketFull 签发携带全部可选 claim(region/cell/role)的 DS 票据。
//
// roleID:玩家已选角色(选角权威化 2026-07-08)。hub 票据传已选角;battle 票据传 0
// (对局角色走 match roster,不走票据)。0 时 claim 不序列化(omitempty),与旧票完全兼容。
//
// 其余语义同 SignDSTicketWithCell。不变量 §3:默认 TTL=5min。
func (s *Signer) SignDSTicketFull(playerID uint64, dsType DSType, matchID uint64, regionID, cellID, roleID uint32, jti string) (token string, expiresAtMs int64, err error) {
	return s.signDSTicket(playerID, dsType, matchID, regionID, cellID, roleID, 0, jti, DSTicketBinding{})
}

// SignHubDSTicketFull 与 SignDSTicketFull(dsType=hub) 相同,另把 Battle→Hub 回流 fence
// (sourceMatchID,见 DSTicketClaims.SourceMatchID)盖进票据。0 = 普通登录(claim 不序列化)。
func (s *Signer) SignHubDSTicketFull(playerID uint64, regionID, cellID, roleID uint32, sourceMatchID uint64, jti string) (token string, expiresAtMs int64, err error) {
	return s.signDSTicket(playerID, DSTypeHub, 0, regionID, cellID, roleID, sourceMatchID, jti, DSTicketBinding{})
}

// SignBoundHubDSTicket 签发绑定当前 Hub DS active 凭据与玩家归属版本的 hub 票据。
// 绑定不完整一律拒绝，避免产生看似新格式但实际可跨实例/跨归属使用的半绑定票据。
// sourceMatchID:Battle→Hub 回流 fence,0 = 普通登录。
func (s *Signer) SignBoundHubDSTicket(playerID uint64, regionID, cellID, roleID uint32, sourceMatchID uint64, jti string, binding DSTicketBinding) (token string, expiresAtMs int64, err error) {
	return s.signDSTicket(playerID, DSTypeHub, 0, regionID, cellID, roleID, sourceMatchID, jti, binding)
}

func (s *Signer) signDSTicket(playerID uint64, dsType DSType, matchID uint64, regionID, cellID, roleID uint32, sourceMatchID uint64, jti string, binding DSTicketBinding) (token string, expiresAtMs int64, err error) {
	if playerID == 0 {
		return "", 0, errors.New("auth.SignDSTicket: playerID must be > 0")
	}
	if dsType != DSTypeHub && dsType != DSTypeBattle {
		return "", 0, fmt.Errorf("auth.SignDSTicket: invalid dsType %q", dsType)
	}
	if jti == "" {
		return "", 0, errors.New("auth.SignDSTicket: jti must be non-empty")
	}
	if dsType == DSTypeBattle && matchID == 0 {
		return "", 0, errors.New("auth.SignDSTicket: battle DSTicket requires matchID")
	}
	if dsType == DSTypeBattle && sourceMatchID != 0 {
		return "", 0, errors.New("auth.SignDSTicket: battle DSTicket must not carry source_match_id")
	}
	if !binding.empty() {
		if dsType != DSTypeHub || !binding.complete() {
			return "", 0, errors.New("auth.SignDSTicket: hub binding must be complete and only used by hub tickets")
		}
	}
	now := s.cfg.NowFn()
	exp := now.Add(s.cfg.DSTicketTTL)
	claims := DSTicketClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.cfg.Issuer,
			Subject:   strconv.FormatUint(playerID, 10),
			Audience:  jwt.ClaimStrings{s.cfg.Audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
			ID:        jti,
		},
		DSType:          string(dsType),
		MatchID:         matchID,
		RegionID:        regionID,
		CellID:          cellID,
		RoleID:          roleID,
		DSPodName:       binding.DSPodName,
		DSInstanceUID:   binding.DSInstanceUID,
		DSProtocolEpoch: binding.ProtocolEpoch,
		DSCredentialGen: binding.CredentialGen,
		DSCredentialJTI: binding.CredentialJTI,
		HubAssignmentID: binding.HubAssignmentID,
		DSWriterEpoch:   binding.WriterEpoch,
		SourceMatchID:   sourceMatchID,
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	str, err := t.SignedString(s.cfg.Secret)
	if err != nil {
		return "", 0, fmt.Errorf("auth.SignDSTicket: %w", err)
	}
	return str, exp.UnixMilli(), nil
}

func (b DSTicketBinding) empty() bool {
	return b.DSPodName == "" && b.DSInstanceUID == "" && b.ProtocolEpoch == 0 &&
		b.CredentialGen == 0 && b.CredentialJTI == "" && b.HubAssignmentID == "" && b.WriterEpoch == 0
}

func (b DSTicketBinding) complete() bool {
	return b.DSPodName != "" && b.DSInstanceUID != "" && b.ProtocolEpoch != 0 &&
		b.CredentialGen != 0 && b.CredentialJTI != "" && b.HubAssignmentID != "" && b.WriterEpoch != 0
}

// SignDSCallback 签发 DS 回调服务令牌(方向:DS→后端;详见 DSCallbackClaims 注释)。
//
// 约束:
//   - dsType=battle:matchID 必填(授权范围 = 本场对局),pod 可空(分配时未知)
//   - dsType=hub:pod 必填(授权范围 = 本实例),matchID 必须为 0
//   - ttl 必须 > 0(battle 默认 4h / hub 默认 24h 由 config.DSAuthConf 决定,调用方传入)
//
// 注意:签发用的 Signer 必须以 DS 回调专用 Config 构造(Issuer=DSCallbackIssuer /
// Audience=DSCallbackAudience 或 ds_auth 配置值),不要复用玩家令牌 Signer。
func (s *Signer) SignDSCallback(dsType DSType, pod string, matchID uint64, ttl time.Duration) (token string, expiresAtMs int64, err error) {
	return s.SignDSCallbackWithGen(dsType, pod, matchID, 0, ttl)
}

// SignDSCallbackWithGen 与 SignDSCallback 相同,但额外把 hub 令牌代际 gen 签进 ds_gen claim。
//
// gen=0 等价于 SignDSCallback(不带代际;battle 令牌 / 未启用代际门控)。gen>0 仅用于
// hub 令牌:hub_allocator 经 Redis INCR 领取严格递增的 gen 后签进来,DS 心跳原样回显,
// 服务端精确相等比较判定是否当前代际(替代秒级 exp 代际,消除同秒重签碰撞;审核 P1-6)。
func (s *Signer) SignDSCallbackWithGen(dsType DSType, pod string, matchID, gen uint64, ttl time.Duration) (token string, expiresAtMs int64, err error) {
	switch dsType {
	case DSTypeBattle:
		if matchID == 0 {
			return "", 0, errors.New("auth.SignDSCallback: battle token requires matchID")
		}
	case DSTypeHub:
		if pod == "" {
			return "", 0, errors.New("auth.SignDSCallback: hub token requires pod")
		}
		if matchID != 0 {
			return "", 0, errors.New("auth.SignDSCallback: hub token must not carry matchID")
		}
	default:
		return "", 0, fmt.Errorf("auth.SignDSCallback: invalid dsType %q", dsType)
	}
	if ttl <= 0 {
		return "", 0, errors.New("auth.SignDSCallback: ttl must be > 0")
	}
	now := s.cfg.NowFn()
	exp := now.Add(ttl)
	kid := keyFingerprint(s.cfg.Secret)
	claims := DSCallbackClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.cfg.Issuer,
			Subject:   pod,
			Audience:  jwt.ClaimStrings{s.cfg.Audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
		DSType:  string(dsType),
		MatchID: matchID,
		DSGen:   gen,
		DSKid:   kid,
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	// 打 kid = 主密钥指纹:令牌自描述用了哪把密钥,轮换期校验侧据此路由到对应密钥(审核 P1 #3)。
	// DS 回调令牌不经 Envoy jwt_authn(只由 DSCallbackGuard 校验),加 kid 头对边缘网关无影响。
	t.Header["kid"] = kid
	str, err := t.SignedString(s.cfg.Secret)
	if err != nil {
		return "", 0, fmt.Errorf("auth.SignDSCallback: %w", err)
	}
	return str, exp.UnixMilli(), nil
}

// HubCredentialResult 是 SignHubCredential 的产物:除令牌串外,还回显组装 HubDSCredential
// 所需的身份字段(kid 在令牌头不在 payload,token_sha256 需签后算),免调用方重解析。
type HubCredentialResult struct {
	Token       string
	ExpMs       int64
	Kid         string // 签发密钥指纹(= keyFingerprint(主密钥))
	TokenSHA256 string // 令牌串 SHA256(hex),完整性绑定
	WriterEpoch uint32
}

// SignHubCredential 签发 Model B 的 hub 回调凭据令牌(decision-revisit-ds-callback-auth §7)。
//
// 与 SignDSCallbackWithGen 相比,额外把 DS 实例身份(instanceUID)、协议纪元(epoch)、jti 签进去,
// 使凭据身份 = (instance_uid, protocol_epoch, gen, jti) 四元组(gen 单独不安全)。jti 由调用方
// 传入(uuid v4,与其他票据签发一致,pkg/auth 不引入 uuid 依赖)。生产 Redis Model B 与
// Windows local-off-v1 都使用本方法；两者只在“是否需要 Redis pending→active ACK”上不同。
func (s *Signer) SignHubCredential(pod, instanceUID string, epoch uint32, gen uint64, jti string, ttl time.Duration) (HubCredentialResult, error) {
	if pod == "" {
		return HubCredentialResult{}, errors.New("auth.SignHubCredential: pod must be non-empty")
	}
	if instanceUID == "" {
		return HubCredentialResult{}, errors.New("auth.SignHubCredential: instanceUID must be non-empty")
	}
	if epoch == 0 {
		return HubCredentialResult{}, errors.New("auth.SignHubCredential: protocol epoch must be > 0")
	}
	if gen == 0 {
		return HubCredentialResult{}, errors.New("auth.SignHubCredential: gen must be > 0")
	}
	if jti == "" {
		return HubCredentialResult{}, errors.New("auth.SignHubCredential: jti must be non-empty")
	}
	if ttl <= 0 {
		return HubCredentialResult{}, errors.New("auth.SignHubCredential: ttl must be > 0")
	}
	now := s.cfg.NowFn()
	exp := now.Add(ttl)
	kid := keyFingerprint(s.cfg.Secret)
	claims := DSCallbackClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.cfg.Issuer,
			Subject:   pod,
			Audience:  jwt.ClaimStrings{s.cfg.Audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
			ID:        jti,
		},
		DSType:          string(DSTypeHub),
		DSGen:           gen,
		DSInstanceUID:   instanceUID,
		DSProtocolEpoch: epoch,
		DSWriterEpoch:   DSAuthWriterEpochV2,
		DSKid:           kid,
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	t.Header["kid"] = kid
	str, err := t.SignedString(s.cfg.Secret)
	if err != nil {
		return HubCredentialResult{}, fmt.Errorf("auth.SignHubCredential: %w", err)
	}
	sum := sha256.Sum256([]byte(str))
	return HubCredentialResult{
		Token: str,
		// jwt.NumericDate 按库的 TimePrecision(默认秒)序列化；Redis 必须保存实际 claim 值，
		// 不能保存未截断的本地 exp，否则严格 active-state 比较会稳定差 1-999ms。
		ExpMs:       claims.ExpiresAt.UnixMilli(),
		Kid:         kid,
		TokenSHA256: hex.EncodeToString(sum[:]),
		WriterEpoch: DSAuthWriterEpochV2,
	}, nil
}

// SignBattleCredential 签发 Model B battle 回调凭据。与 Hub 凭据相同地绑定
// (pod, instance_uid, instance_epoch, gen, jti, writer_epoch)，并额外绑定 match_id。
// Agones 路径必须先选中 GameServer 并经 GET 取得 UID；local-off-v1 则由本地 allocator
// 为每个进程生成唯一 UUID，并以 epoch/gen=1 的单次实例凭据调用。
func (s *Signer) SignBattleCredential(matchID uint64, pod, instanceUID string, epoch uint32, gen uint64, jti string, ttl time.Duration) (HubCredentialResult, error) {
	if matchID == 0 {
		return HubCredentialResult{}, errors.New("auth.SignBattleCredential: matchID must be > 0")
	}
	if pod == "" {
		return HubCredentialResult{}, errors.New("auth.SignBattleCredential: pod must be non-empty")
	}
	if instanceUID == "" {
		return HubCredentialResult{}, errors.New("auth.SignBattleCredential: instanceUID must be non-empty")
	}
	if epoch == 0 || gen == 0 || jti == "" || ttl <= 0 {
		return HubCredentialResult{}, errors.New("auth.SignBattleCredential: epoch/gen/jti/ttl must be non-zero")
	}
	now := s.cfg.NowFn()
	exp := now.Add(ttl)
	kid := keyFingerprint(s.cfg.Secret)
	claims := DSCallbackClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: s.cfg.Issuer, Subject: pod, Audience: jwt.ClaimStrings{s.cfg.Audience},
			IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(exp), ID: jti,
		},
		DSType: string(DSTypeBattle), MatchID: matchID, DSGen: gen,
		DSInstanceUID: instanceUID, DSProtocolEpoch: epoch,
		DSWriterEpoch: DSAuthWriterEpochV2, DSKid: kid,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = kid
	encoded, err := token.SignedString(s.cfg.Secret)
	if err != nil {
		return HubCredentialResult{}, fmt.Errorf("auth.SignBattleCredential: %w", err)
	}
	sum := sha256.Sum256([]byte(encoded))
	return HubCredentialResult{
		Token: encoded, ExpMs: claims.ExpiresAt.UnixMilli(), Kid: kid,
		TokenSHA256: hex.EncodeToString(sum[:]), WriterEpoch: DSAuthWriterEpochV2,
	}, nil
}

// VerifySession 校验 SessionToken,返回 claims。
//
// 失败返回 *errcode.Error(ErrLoginTicketExpired / ErrLoginTicketInvalid),
// 业务侧用 errcode.As 转 proto code。
func (v *Verifier) VerifySession(token string) (*SessionClaims, error) {
	var claims SessionClaims
	if err := v.parseInto(token, &claims); err != nil {
		return nil, err
	}
	if claims.PlayerID() == 0 {
		return nil, errcode.New(errcode.ErrLoginTicketInvalid, "session sub not a valid player_id")
	}
	return &claims, nil
}

// VerifyAccount 校验**账号态 token**,返回 claims。
//
// 按 AccountAudience 校验 aud:玩家态 SessionToken 到这里必然失败(aud 不符),
// 反之账号态 token 也过不了 VerifySession。这条互斥性有 TestAccountAndSessionTokensAreNotInterchangeable 钉住。
func (v *Verifier) VerifyAccount(token string) (*AccountClaims, error) {
	var claims AccountClaims
	if err := v.parseIntoAudience(token, &claims, v.cfg.AccountAudience); err != nil {
		return nil, err
	}
	if claims.AccountID() == 0 {
		return nil, errcode.New(errcode.ErrLoginTicketInvalid, "account sub not a valid account_id")
	}
	return &claims, nil
}

// VerifyDSTicket 校验 DSTicket,返回 claims。
//
// 校验项:
//   - 签名 / exp / iss / aud(parseInto)
//   - sub 为有效 player_id
//   - ds_type 必在 "hub" / "battle"
//   - dsType=battle 时 match_id 必非 0(与 SignDSTicket 防御性检查对称,防伪造 token 跳过 sign 分支)
//
// 防重放(jti 黑名单)需要调用方再走一次 redis SET NX EX 检查;本方法只验签 + exp。
func (v *Verifier) VerifyDSTicket(token string) (*DSTicketClaims, error) {
	var claims DSTicketClaims
	if err := v.parseInto(token, &claims); err != nil {
		return nil, err
	}
	if claims.PlayerID() == 0 {
		return nil, errcode.New(errcode.ErrLoginTicketInvalid, "ds ticket sub not a valid player_id")
	}
	if claims.DSType != string(DSTypeHub) && claims.DSType != string(DSTypeBattle) {
		return nil, errcode.New(errcode.ErrLoginTicketInvalid, "ds ticket dsType invalid: %q", claims.DSType)
	}
	if claims.DSType == string(DSTypeBattle) && claims.MatchID == 0 {
		return nil, errcode.New(errcode.ErrLoginTicketInvalid, "battle ds ticket missing match_id")
	}
	binding := DSTicketBinding{
		DSPodName:       claims.DSPodName,
		DSInstanceUID:   claims.DSInstanceUID,
		ProtocolEpoch:   claims.DSProtocolEpoch,
		CredentialGen:   claims.DSCredentialGen,
		CredentialJTI:   claims.DSCredentialJTI,
		HubAssignmentID: claims.HubAssignmentID,
		WriterEpoch:     claims.DSWriterEpoch,
	}
	if !binding.empty() && (claims.DSType != string(DSTypeHub) || !binding.complete()) {
		return nil, errcode.New(errcode.ErrLoginTicketInvalid, "ds ticket has incomplete or invalid hub binding")
	}
	return &claims, nil
}

// VerifyDSCallback 校验 DS 回调服务令牌,返回 claims。
//
// 校验项:
//   - 签名 / exp / iss / aud(parseInto;aud 分域保证玩家令牌 / DSTicket 在此必然失败)
//   - ds_type 必在 "hub" / "battle"
//   - battle → match_id 非 0;hub → sub(pod)非空(与 SignDSCallback 防御性检查对称)
//
// 失败统一返回 errcode.ErrUnauthorized(区别于玩家票据的 LoginTicket 错误码)。
// 范围绑定(match_id / pod 与请求参数一致)由 pkg/middleware.DSCallbackGuard 做。
func (v *Verifier) VerifyDSCallback(token string) (*DSCallbackClaims, error) {
	var claims DSCallbackClaims
	if err := v.parseInto(token, &claims); err != nil {
		return nil, errcode.New(errcode.ErrUnauthorized, "ds callback token: %v", err)
	}
	switch claims.DSType {
	case string(DSTypeBattle):
		if claims.MatchID == 0 {
			return nil, errcode.New(errcode.ErrUnauthorized, "battle ds callback token missing match_id")
		}
	case string(DSTypeHub):
		if claims.Subject == "" {
			return nil, errcode.New(errcode.ErrUnauthorized, "hub ds callback token missing pod (sub)")
		}
	default:
		return nil, errcode.New(errcode.ErrUnauthorized, "ds callback token dsType invalid: %q", claims.DSType)
	}
	return &claims, nil
}

// parseInto 把 token 解到 dst claims;统一翻译标准 jwt 错误到 errcode。
func (v *Verifier) parseInto(token string, dst jwt.Claims) error {
	return v.parseIntoAudience(token, dst, v.cfg.Audience)
}

// parseIntoAudience 是 parseInto 的显式 audience 版本。
//
// 拆出来只为一件事:账号态 token 与玩家态 SessionToken 用**不同 aud** 隔离
// (见 Config.AccountAudience)。aud 校验必须留在这一层(jwt.WithAudience),
// 与签名校验同层原子完成;搬到调用方去比对 claims.Audience 就变成了「验完签再判别」,
// 漏判一处即越权。
func (v *Verifier) parseIntoAudience(token string, dst jwt.Claims, audience string) error {
	if token == "" {
		return errcode.New(errcode.ErrLoginTicketInvalid, "empty token")
	}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(v.cfg.Issuer),
		jwt.WithAudience(audience),
		// exp 必须存在:所有 Sign* 都写 exp,校验侧强制要求,杜绝「签名正确但无过期时间 = 永久有效」
		// 的令牌(审核 P1:DS 回调令牌若无 exp 即成永久凭证)。
		jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(v.cfg.NowFn),
	)
	_, err := parser.ParseWithClaims(token, dst, func(t *jwt.Token) (interface{}, error) {
		keys := v.cfg.verificationKeys()
		// 有 kid 且能匹配到某把校验密钥 → 直接路由到它(轮换期确定性选键)。
		if kid, _ := t.Header["kid"].(string); kid != "" {
			for _, k := range keys {
				if keyFingerprint(k) == kid {
					return k, nil
				}
			}
		}
		// 单密钥:与历史行为完全一致(直接返回该密钥)。
		if len(keys) == 1 {
			return keys[0], nil
		}
		// 多密钥且无 / 未知 kid(如轮换前签发的旧 token):依次尝试全部密钥(golang-jwt v5 VerificationKeySet)。
		set := jwt.VerificationKeySet{Keys: make([]jwt.VerificationKey, len(keys))}
		for i := range keys {
			set.Keys[i] = keys[i]
		}
		return set, nil
	})
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, jwt.ErrTokenExpired):
		return errcode.New(errcode.ErrLoginTicketExpired, "token expired: %v", err)
	case errors.Is(err, jwt.ErrTokenNotValidYet),
		errors.Is(err, jwt.ErrTokenSignatureInvalid),
		errors.Is(err, jwt.ErrTokenInvalidIssuer),
		errors.Is(err, jwt.ErrTokenInvalidAudience),
		errors.Is(err, jwt.ErrTokenMalformed):
		return errcode.New(errcode.ErrLoginTicketInvalid, "token invalid: %v", err)
	default:
		return errcode.New(errcode.ErrLoginTicketInvalid, "token parse failed: %v", err)
	}
}

// JWKSInlineHS256 用 Config.Secret 输出一份 Envoy jwt_authn local_jwks 可直接 inline 的 JSON。
//
// Envoy jwt_authn 接受 RFC 7517 JWKS 格式,HS256 用 `kty=oct` + `k=base64url(secret)`。
//
// 用法(deploy/envoy/envoy.yaml):
//
//	providers:
//	  pandora_session:
//	    issuer: pandora-login
//	    audiences: [pandora-client]
//	    local_jwks:
//	      inline_string: |
//	        {"keys":[{"kty":"oct","alg":"HS256","kid":"pandora-dev","k":"<base64url>"}]}
//
// 本函数返回上面 inline_string 的内容,便于把 secret 跟 envoy.yaml 同步起来时只改一处。
func JWKSInlineHS256(secret []byte, kid string) string {
	if kid == "" {
		kid = "pandora-dev"
	}
	k := base64.RawURLEncoding.EncodeToString(secret)
	return fmt.Sprintf(`{"keys":[{"kty":"oct","alg":"HS256","kid":%q,"k":%q}]}`, kid, k)
}

// SecretEqual 常量时间比较两个 secret;dev / 配置检查用。
func SecretEqual(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}

// AdditionalSecretsBytes 把 yaml 配置里的 additional_secrets([]string)原样转 [][]byte。
// 刻意**不过滤空串**:空条目是轮换清单事故,交给 Config.Validate 报错(fail-closed,二审 #8),
// 静默过滤会让运维以为旧密钥仍被接受、实则轮换断档。
func AdditionalSecretsBytes(list []string) [][]byte {
	if len(list) == 0 {
		return nil
	}
	out := make([][]byte, len(list))
	for i, s := range list {
		out[i] = []byte(s)
	}
	return out
}

// AssertDisjointSecrets 断言玩家面密钥集与 DS 回调面密钥集完全不相交(P0 审核:
// 任何一把密钥同时出现在两面时,泄露玩家面即可伪造 DS 回调令牌绕过范围绑定,反之亦然)。
// 两个集合各自包含「主密钥 + 全部 additional 校验密钥」;同时装配两面的服务
// (hub_allocator)启动期必须调用,任一交叉即返回错误(fail-closed,启动即拒)。
func AssertDisjointSecrets(playerKeys, dsKeys [][]byte) error {
	for i, p := range playerKeys {
		for j, d := range dsKeys {
			if SecretEqual(p, d) {
				return fmt.Errorf("auth: player-facing key[%d] equals ds-callback key[%d] (P0: 玩家面与 DS 回调面密钥集必须不相交)", i, j)
			}
		}
	}
	return nil
}
