// Package biz 是 mail 服务的业务逻辑层(2026-06-29)。
//
// 职责(docs/design/mail.md):
//   - ListMail:个人邮件(写扩散)+ 系统/公会邮件(channel+watermark 拉取)合并视图,
//     拉取后推进游标(last_sys/last_guild),实现"看过的不重复拉、过期的不拉"
//   - ReadMail:个人邮件置已读;系统/公会邮件推进游标
//   - ClaimMail:附件领取,player_mail_claim 幂等(同 mail+player 只发一次)
//   - SendSystemMail/SendGuildMail:只插一行(零写扩散,僵尸/退游不登录即零成本)
//   - SendPersonalMail:写收件人收件箱(离线可达)
//
// 客户端只拿 Mail / MailAttachment 视图(CLAUDE.md §14):正文+附件存 payload blob,
// 服务端解包成最小视图返回。
package biz

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
	mailv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mail/v1"

	"github.com/luyuancpp/pandora/services/social/mail/internal/conf"
	"github.com/luyuancpp/pandora/services/social/mail/internal/data"
)

// ItemGranter 把附件入背包(由 inventory 服务实现,幂等键防重发)。
type ItemGranter interface {
	Grant(ctx context.Context, playerID uint64, atts []*mailv1.MailAttachment, idempotencyKey string) error
}

// InstanceGranter 把 instance 形态附件(有唯一 ID 的物品/装备)按 count 逐件铸造为独立实例
// (inventory.GrantInstances;铸出默认未鉴定,词条在鉴定时才掷),幂等键防重发。
// 战斗装备掉落背包满转邮件用此路径,保持装备=唯一实例语义。
// 实现可为 nil:未配 inventory 时实例型附件领取直接报错(不误发成堆叠)。
type InstanceGranter interface {
	GrantInstances(ctx context.Context, playerID uint64, itemConfigIDs []uint32, idempotencyKey string) error
}

// TransferClaimer 把 transfer 形态附件(既存实例托管转移)交付给领取人:调 inventory
// ClaimTransferInstances 把托管行逐字节原样搬进领取人实例表(只改归属,零重铸零重 roll;
// bag-domain.md §7.1)。领取只认托管行,附件快照仅供核对(instance_id+config)。
// 实现可为 nil:未配 inventory 时 transfer 附件领取直接报错——不允许空领(AllowNoopGrant
// 也不放行):空领会把邮件标成已领而托管行原地不动,实例资产静默滞留 escrow。
type TransferClaimer interface {
	ClaimTransfers(ctx context.Context, playerID uint64, atts []*mailv1.MailAttachment, idempotencyKey string) error
}

// TransferEscrowConsumer 消 transfer 托管行不物化(bag phase 2 DS 领取链,MarkMailClaimed 调):
// 资产已经 bag journal 原样入包,经济域托管行只删,防实例双持。行缺失 no-op 幂等。
type TransferEscrowConsumer interface {
	ConsumeTransferEscrow(ctx context.Context, playerID uint64, instanceIDs []uint64) error
}

// instanceIDGen DS 领取意图展开时给 instance 形态附件铸实例 ID(雪花;一次铸定,
// 意图落库后重放返回同一批 ID——bag journal 内容指纹去重的前提)。
//
// GenerateInto 是批量预留:一次 CAS 拿整段,替代按件数循环调用 Generate。
// 语义与逐个 Generate 完全一致(严格递增、互不重复、可与 Generate 混用),
// 只保证「递增 + 唯一」,不保证相邻连续(跨秒会有空洞)。
type instanceIDGen interface {
	Generate() uint64
	GenerateInto(dst []uint64)
}

// MailUsecase 是 mail 服务业务逻辑核心。
type MailUsecase struct {
	repo        data.MailRepo
	cfg         conf.MailConf
	granter     ItemGranter
	instGranter InstanceGranter
	xferClaimer TransferClaimer

	// bag phase 2 DS 三段式领取(GetClaimableAttachments / MarkMailClaimed):
	escrowConsumer TransferEscrowConsumer // Mark 时消 transfer 托管行(nil = 含 transfer 的意图拒终结)
	idGen          instanceIDGen          // 意图展开铸 instance ID(nil = 含 instance 的意图拒创建)
}

// NewMailUsecase 构造。granter 为 nil 时仅允许 AllowNoopGrant 配置下空领(测试用)。
// instGranter 用 setter 注入(SetInstanceGranter),保持构造签名不变。
func NewMailUsecase(repo data.MailRepo, cfg conf.MailConf, granter ItemGranter) *MailUsecase {
	// 归一化实例件数上限:零值必须退回默认值,而不是变成「拒绝一切实例附件」。
	// conf.Defaults() 已覆盖 yaml 路径,这里再兜一次直接构造 MailConf 的调用方
	// (测试、内嵌装配),避免漏配把正常业务静默打死(§14.2)。
	if cfg.MaxInstancesPerMail <= 0 {
		cfg.MaxInstancesPerMail = conf.DefaultMaxInstancesPerMail
	}
	return &MailUsecase{repo: repo, cfg: cfg, granter: granter}
}

// SetInstanceGranter 注入实例发放器(领取 instance 形态附件用,nil-safe)。
// nil / 不调用 = 装备型附件领取报错(不会误发成可堆叠计数,守住装备实例不变量)。
func (u *MailUsecase) SetInstanceGranter(g InstanceGranter) {
	u.instGranter = g
}

// SetTransferClaimer 注入托管转移交付器(领取 transfer 形态附件用,nil-safe)。
// nil / 不调用 = transfer 附件领取报错(严格拒,无空领豁免:见 TransferClaimer 注释)。
func (u *MailUsecase) SetTransferClaimer(c TransferClaimer) {
	u.xferClaimer = c
}

// SetTransferEscrowConsumer 注入托管消费器(DS 三段式 Mark 用,nil-safe:
// nil 时含 transfer 的意图拒终结,防托管行残留双持)。
func (u *MailUsecase) SetTransferEscrowConsumer(c TransferEscrowConsumer) {
	u.escrowConsumer = c
}

// SetInstanceIDGen 注入实例 ID 生成器(DS 三段式意图展开铸 instance ID 用,nil-safe:
// nil 时含 instance 形态附件的意图拒创建,不误发)。
func (u *MailUsecase) SetInstanceIDGen(g instanceIDGen) {
	u.idGen = g
}

// 分页上限(决策:docs/design/decision-revisit-list-pagination.md)。
const (
	defaultPageLimit = 50
	maxPageLimit     = 100
)

// clampLimit 把 0 归默认、超上限收敛。
func clampLimit(limit int) int {
	if limit <= 0 {
		return defaultPageLimit
	}
	if limit > maxPageLimit {
		return maxPageLimit
	}
	return limit
}

// ListMail 合并三类邮件,分页拉取个人邮件,首页(cursor=0)拼系统/公会 watermark 增量。
// nextCursor 为本页末个人邮件 mail_id;0=个人邮件无更多。
func (u *MailUsecase) ListMail(ctx context.Context, playerID uint64, nowMs int64, cursor uint64, limit int) ([]*mailv1.Mail, uint64, error) {
	limit = clampLimit(limit)
	lastSys, lastGuild, err := u.repo.GetCursor(ctx, playerID)
	if err != nil {
		return nil, 0, err
	}

	var out []*mailv1.Mail
	maxSys, maxGuild := lastSys, lastGuild

	personal, err := u.repo.ListPersonal(ctx, playerID, nowMs, cursor, limit)
	if err != nil {
		return nil, 0, err
	}
	var nextCursor uint64
	if len(personal) == limit && limit > 0 {
		nextCursor = personal[len(personal)-1].MailID
	}
	for _, m := range personal {
		out = append(out, toMail(m, mailv1.MailChannel_MAIL_CHANNEL_PERSONAL, m.Status, m.Claimed))
	}

	// 系统/公会邮件靠 watermark 天然有界,仅首页拼接,翻页只走个人邮件。
	if cursor != 0 {
		return out, nextCursor, nil
	}

	sys, err := u.repo.ListSysSince(ctx, lastSys, nowMs)
	if err != nil {
		return nil, 0, err
	}
	for _, m := range sys {
		out = append(out, u.toChannelMail(ctx, playerID, m, mailv1.MailChannel_MAIL_CHANNEL_SYSTEM))
		if m.MailID > maxSys {
			maxSys = m.MailID
		}
	}

	if gid, ok, err := u.repo.GetPlayerGuild(ctx, playerID); err != nil {
		return nil, 0, err
	} else if ok {
		guildMails, err := u.repo.ListGuildSince(ctx, gid, lastGuild, nowMs)
		if err != nil {
			return nil, 0, err
		}
		for _, m := range guildMails {
			out = append(out, u.toChannelMail(ctx, playerID, m, mailv1.MailChannel_MAIL_CHANNEL_GUILD))
			if m.MailID > maxGuild {
				maxGuild = m.MailID
			}
		}
	}

	if maxSys > lastSys || maxGuild > lastGuild {
		if err := u.repo.AdvanceCursor(ctx, playerID, maxSys, maxGuild); err != nil {
			return nil, 0, err
		}
	}
	return out, nextCursor, nil
}

// ReadMail 个人邮件置已读;系统/公会邮件靠游标(ListMail 已推进),此处幂等返回。
func (u *MailUsecase) ReadMail(ctx context.Context, playerID, mailID uint64) error {
	return u.repo.SetPersonalStatus(ctx, playerID, mailID, data.StatusRead)
}

// ClaimMail 领附件,幂等。返回实发清单(无附件返回空)。
//
// 安全:GetClaimablePayload 按 channel 校验领取人权限(个人=收件人本人 / 系统=任意 /
// 公会=当前会员)+ 生效区间,越权直接 NotFound。
// 顺序:先校验 → 先调 inventory 入库(幂等键 mail:{mail}:{player},inventory 自身幂等)→
// 入库成功后写 player_mail_claim 标记;crash 在写标记前不致丢奖(下次重领靠 inventory 幂等去重)。
func (u *MailUsecase) ClaimMail(ctx context.Context, playerID, mailID uint64, nowMs int64) ([]*mailv1.MailAttachment, error) {
	payload, found, err := u.repo.GetClaimablePayload(ctx, playerID, mailID, nowMs)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errcode.New(errcode.ErrMailNotFound, "mail %d not found or not claimable", mailID)
	}
	rec := &mailv1.MailContentStorageRecord{}
	if err := proto.Unmarshal(payload, rec); err != nil {
		return nil, errcode.New(errcode.ErrInternal, "decode mail %d: %v", mailID, err)
	}
	if len(rec.GetAttachments()) == 0 {
		return nil, errcode.New(errcode.ErrMailNoAttachment, "mail %d has no attachment", mailID)
	}
	if claimed, intentOpen, err := u.repo.GetClaimState(ctx, playerID, mailID); err != nil {
		return nil, err
	} else if claimed {
		return rec.GetAttachments(), errcode.New(errcode.ErrMailAlreadyClaimed, "mail %d already claimed", mailID)
	} else if intentOpen {
		// DS 三段式领取意图已创建(bag phase 2):本邮件只能经 bag journal 链终结。
		// 旧直连链在此互斥拒——若继续走 inventory 发放,与已/将落库的 journal 双发。
		return nil, errcode.New(errcode.ErrMailClaimInProgress,
			"mail %d claim in progress via bag journal", mailID)
	}
	// 入库:幂等键保证重复领取/重试不重发(资产不变量 §7)。
	// 附件按 oneof 形态分三类:stack → GrantItems;instance → GrantInstances(铸新实例);
	// transfer → ClaimTransferInstances(托管行只改归属,bag-domain.md §7.1)。
	// 各用独立幂等键,重领/重试各自去重;发放顺序 stack → instance → transfer,
	// 任一步失败下次重领靠幂等去重不重发。
	// body 未识别(滚更共存窗口旧端读到新增形态,§9.21)→ 整封 fail-closed 保持未领取,
	// 不发放任何附件、不记 claim,禁止静默跳过(否则新形态附件被吞)。
	stackAtts, instAtts, xferAtts, unknown := partitionAttachments(rec.GetAttachments())
	if unknown > 0 {
		// 滚动版本偏斜的 fail-closed 分支(§9.21):新版本写入了旧 reader 不认识的附件形态,
		// 整封拒发保持未领。这是**必须被运维发现**的信号(说明有旧副本在读新数据),
		// 但只返回业务码时 access log 只有泛化失败,定位不到是哪封 / 几个未知形态。
		plog.With(ctx).Warnw("msg", "mail_claim_unknown_attachment",
			"player_id", playerID, "mail_id", mailID, "unknown", unknown,
			"hint", "疑似滚动升级版本偏斜:旧副本读到新增附件形态,整封 fail-closed 保持未领")
		return nil, errcode.New(errcode.ErrMailAttachmentUnsupported,
			"mail %d has %d unrecognized attachment kind(s)", mailID, unknown)
	}
	if len(stackAtts) > 0 {
		key := fmt.Sprintf("mail:%d:%d", mailID, playerID)
		if u.granter != nil {
			if err := u.granter.Grant(ctx, playerID, stackAtts, key); err != nil {
				return nil, err
			}
		} else if !u.cfg.AllowNoopGrant {
			return nil, errcode.New(errcode.ErrInternal, "inventory granter unavailable")
		}
	}
	if len(instAtts) > 0 {
		key := rec.GetInstanceGrantKey()
		if key == "" {
			key = fmt.Sprintf("mail_inst:%d:%d", mailID, playerID)
		}
		if u.instGranter != nil {
			if err := u.instGranter.GrantInstances(ctx, playerID, expandInstanceConfigIDs(instAtts), key); err != nil {
				return nil, err
			}
		} else if !u.cfg.AllowNoopGrant {
			return nil, errcode.New(errcode.ErrInternal, "instance granter unavailable")
		}
	}
	if len(xferAtts) > 0 {
		// transfer 无空领豁免(AllowNoopGrant 不放行):空领 = 邮件标已领而托管行原地滞留,
		// 实例资产静默丢失;宁可领取报错保持可重领。
		if u.xferClaimer == nil {
			return nil, errcode.New(errcode.ErrInternal, "transfer claimer unavailable")
		}
		key := fmt.Sprintf("mail_xfer:%d:%d", mailID, playerID)
		if err := u.xferClaimer.ClaimTransfers(ctx, playerID, xferAtts, key); err != nil {
			return nil, err
		}
	}
	// 入库成功后再记 claim;此处即便失败,下次重领被 inventory 幂等去重,不会重发
	if _, err := u.repo.RecordClaim(ctx, playerID, mailID); err != nil {
		return nil, err
	}
	// 个人邮件置 claimed(系统/公会靠 player_mail_claim 表)
	if serr := u.repo.SetPersonalStatus(ctx, playerID, mailID, data.StatusClaimed); serr != nil {
		// 权威是 claim 表,列表侧幂等纠正;但持续失败会让 player_mail.status 与权威长期漂移
		// (客户端显示未领实际已领),零可观测无法排查 → DEBUG 留证。
		plog.With(ctx).Debugw("msg", "mail_set_personal_status_failed",
			"player_id", playerID, "mail_id", mailID, "target_status", "claimed", "err", serr)
	}
	return rec.GetAttachments(), nil
}

// DeleteMail 删个人邮件。
func (u *MailUsecase) DeleteMail(ctx context.Context, playerID, mailID uint64) error {
	return u.repo.DeletePersonal(ctx, playerID, mailID)
}

// ── DS 三段式领取(bag phase 2,2026-07-22;bag-domain.md §7)──────────────────
//
// 时序(owner DS 驱动):GetClaimableAttachments(意图落库,稳定展开)→ DS 预留容量 +
// bag.AppendJournal(op=mail_claim,幂等键=claim_key,单条批)→ MarkMailClaimed。
// 恰好一次:意图内容持久化(重放逐字节一致 → journal 指纹去重命中即已入包);
// Mark 前任意点崩溃 → 重走 Get(返回同内容)→ journal 重放去重 → Mark 幂等。

// mailClaimKey DS 领取的 journal 幂等键(与意图行同生命周期)。
func mailClaimKey(mailID, playerID uint64) string {
	return fmt.Sprintf("mail_claim:%d:%d", mailID, playerID)
}

// GetClaimableAttachments 取(或幂等重取)领取意图。alreadyClaimed=true 表示已终态
// (items 空,DS 直接刷新 UI);否则返回稳定展开与 claim_key。
func (u *MailUsecase) GetClaimableAttachments(ctx context.Context, playerID, mailID uint64, nowMs int64) (items []*bagv1.BagItem, claimKey string, alreadyClaimed bool, err error) {
	claimKey = mailClaimKey(mailID, playerID)
	payload, found, err := u.repo.GetClaimablePayload(ctx, playerID, mailID, nowMs)
	if err != nil {
		return nil, "", false, err
	}
	if !found {
		return nil, "", false, errcode.New(errcode.ErrMailNotFound, "mail %d not found or not claimable", mailID)
	}
	claimed, intentOpen, err := u.repo.GetClaimState(ctx, playerID, mailID)
	if err != nil {
		return nil, "", false, err
	}
	if claimed {
		return nil, claimKey, true, nil
	}
	if intentOpen {
		return u.loadIntentItems(ctx, playerID, mailID, claimKey)
	}
	rec := &mailv1.MailContentStorageRecord{}
	if uerr := proto.Unmarshal(payload, rec); uerr != nil {
		return nil, "", false, errcode.New(errcode.ErrInternal, "decode mail %d: %v", mailID, uerr)
	}
	if len(rec.GetAttachments()) == 0 {
		return nil, "", false, errcode.New(errcode.ErrMailNoAttachment, "mail %d has no attachment", mailID)
	}
	intent, berr := u.buildClaimIntent(rec)
	if berr != nil {
		return nil, "", false, berr
	}
	blob, merr := proto.Marshal(intent)
	if merr != nil {
		return nil, "", false, errcode.New(errcode.ErrInternal, "encode intent mail %d: %v", mailID, merr)
	}
	created, cerr := u.repo.CreateClaimIntent(ctx, playerID, mailID, blob)
	if cerr != nil {
		return nil, "", false, cerr
	}
	if !created {
		// 并发/重放:行已存在(意图或终态),重读为准——绝不覆盖既有展开
		// (覆盖会换 instance ID,破坏 journal 指纹一致性)。
		claimed, _, serr := u.repo.GetClaimState(ctx, playerID, mailID)
		if serr != nil {
			return nil, "", false, serr
		}
		if claimed {
			return nil, claimKey, true, nil
		}
		return u.loadIntentItems(ctx, playerID, mailID, claimKey)
	}
	return intent.GetItems(), claimKey, false, nil
}

// loadIntentItems 读既有意图行并解包(意图必须存在;缺失 = 状态窗口内被终结,按终态返回)。
func (u *MailUsecase) loadIntentItems(ctx context.Context, playerID, mailID uint64, claimKey string) ([]*bagv1.BagItem, string, bool, error) {
	blob, found, err := u.repo.GetClaimIntent(ctx, playerID, mailID)
	if err != nil {
		return nil, "", false, err
	}
	if !found {
		// 读状态与读意图之间被 Mark 终结(并发重放窗口):按已领返回,幂等安全。
		return nil, claimKey, true, nil
	}
	intent := &mailv1.MailClaimIntentStorageRecord{}
	if uerr := proto.Unmarshal(blob, intent); uerr != nil {
		return nil, "", false, errcode.New(errcode.ErrInternal, "decode intent mail %d: %v", mailID, uerr)
	}
	return intent.GetItems(), claimKey, false, nil
}

// buildClaimIntent 把附件展开为稳定 BagItem 列表(instance 形态在此一次性铸 ID)。
// 任一附件未识别 → 整封 fail-closed 9606(与直连链同语义,不静默跳过)。
func (u *MailUsecase) buildClaimIntent(rec *mailv1.MailContentStorageRecord) (*mailv1.MailClaimIntentStorageRecord, error) {
	intent := &mailv1.MailClaimIntentStorageRecord{}
	var instanceTotal uint64 // instance 形态跨全部附件的累计件数
	for i, a := range rec.GetAttachments() {
		switch {
		case a.GetStack() != nil:
			s := a.GetStack()
			intent.Items = append(intent.Items, &bagv1.BagItem{ItemConfigId: s.GetItemConfigId(), Count: s.GetCount()})
		case a.GetInstance() != nil:
			inst := a.GetInstance()
			if u.idGen == nil {
				return nil, errcode.New(errcode.ErrInternal, "instance id generator unavailable")
			}
			n := inst.GetCount()
			if n == 0 {
				n = 1
			}
			// 存量行兜底:同口径上限在发送侧已拦截,但本次改动**之前**入库的邮件
			// 可能带着无界 count。必须在分配与铸号**之前**判定,宁可整封 fail-closed
			// 让运营修数据,也不能在这里把内存吃光(§16.5 容量边界)。
			instanceTotal += uint64(n)
			if instanceTotal > uint64(u.cfg.MaxInstancesPerMail) {
				return nil, errcode.New(errcode.ErrInvalidArg,
					"attachment[%d] instance count exceeds per-mail limit: total=%d max=%d",
					i, instanceTotal, u.cfg.MaxInstancesPerMail)
			}
			// 一次 CAS 预留 n 个 ID,替代循环调用 Generate n 次。
			// 产出严格递增且唯一,但不保证连续——此处只按下标取用,不依赖连续性。
			ids := make([]uint64, n)
			u.idGen.GenerateInto(ids)
			for j := uint32(0); j < n; j++ {
				intent.Items = append(intent.Items, &bagv1.BagItem{
					ItemConfigId: inst.GetItemConfigId(), Count: 1, InstanceId: ids[j],
				})
			}
		case a.GetTransfer() != nil:
			item := a.GetTransfer().GetItem()
			intent.Items = append(intent.Items, item)
			intent.TransferInstanceIds = append(intent.TransferInstanceIds, item.GetInstanceId())
		default:
			return nil, errcode.New(errcode.ErrMailAttachmentUnsupported, "attachment[%d] body required", i)
		}
	}
	return intent, nil
}

// MarkMailClaimed 终结 DS 领取(journal 已 ACK 后调):消 transfer 托管行 → 置终态。
// 幂等:已终态 no-op;无意图且未领取 → ErrInvalidArg(时序违规,journal 前不得 Mark)。
func (u *MailUsecase) MarkMailClaimed(ctx context.Context, playerID, mailID uint64) error {
	claimed, intentOpen, err := u.repo.GetClaimState(ctx, playerID, mailID)
	if err != nil {
		return err
	}
	if claimed {
		return nil
	}
	if !intentOpen {
		// DS 三段式领取的时序违规:journal/GetClaimable 之前就调了 Mark(调用方 bug 或迟到/乱序回调)。
		plog.With(ctx).Warnw("msg", "mail_mark_without_intent", "player_id", playerID, "mail_id", mailID)
		return errcode.New(errcode.ErrInvalidArg, "mail %d has no claim intent to mark", mailID)
	}
	blob, found, err := u.repo.GetClaimIntent(ctx, playerID, mailID)
	if err != nil {
		return err
	}
	if found {
		intent := &mailv1.MailClaimIntentStorageRecord{}
		if uerr := proto.Unmarshal(blob, intent); uerr != nil {
			return errcode.New(errcode.ErrInternal, "decode intent mail %d: %v", mailID, uerr)
		}
		if ids := intent.GetTransferInstanceIds(); len(ids) > 0 {
			// transfer 附件已经 journal 原样入包:先消经济域托管行(幂等,行缺失 no-op),
			// 再置终态。中间崩溃 → 意图仍开 → 重 Mark 重消(恰好一次)。
			if u.escrowConsumer == nil {
				return errcode.New(errcode.ErrInternal, "transfer escrow consumer unavailable")
			}
			if cerr := u.escrowConsumer.ConsumeTransferEscrow(ctx, playerID, ids); cerr != nil {
				return cerr
			}
		}
	}
	if _, merr := u.repo.MarkClaimed(ctx, playerID, mailID); merr != nil {
		return merr
	}
	// 个人邮件置 claimed 状态(系统/公会靠 claim 行;失败不影响终态,列表侧幂等纠正)。
	if serr := u.repo.SetPersonalStatus(ctx, playerID, mailID, data.StatusClaimed); serr != nil {
		// 权威是 claim 表,列表侧幂等纠正;但持续失败会让 player_mail.status 与权威长期漂移
		// (客户端显示未领实际已领),零可观测无法排查 → DEBUG 留证。
		plog.With(ctx).Debugw("msg", "mail_set_personal_status_failed",
			"player_id", playerID, "mail_id", mailID, "target_status", "claimed", "err", serr)
	}
	return nil
}

// SendSystemMail 插一行系统邮件,返回 mail_id(transfer 附件拒:多人可领与单实例矛盾)。
func (u *MailUsecase) SendSystemMail(ctx context.Context, mailID uint64, title, body string, atts []*mailv1.MailAttachment, startMs, endMs, nowMs int64) (uint64, error) {
	payload, err := u.buildPayload(title, body, atts, "", false)
	if err != nil {
		return 0, err
	}
	endMs = u.defaultEnd(startMs, endMs, nowMs)
	if endMs <= startMs {
		// 钳制后窗口无效 = start 定得比「创建 + claim 保留期」还晚(或显式 end<=start),
		// 这种邮件永远不可领,fail-fast 提醒运营改期,不落死信。
		return 0, errcode.New(errcode.ErrInvalidArg,
			"mail window invalid: start=%d end=%d (lifetime capped at claim_retention_days=%d)",
			startMs, endMs, u.cfg.ClaimRetentionDays)
	}
	if err := u.repo.InsertSysMail(ctx, mailID, startMs, endMs, payload); err != nil {
		return 0, err
	}
	return mailID, nil
}

// SendGuildMail 插一行公会邮件(transfer 附件拒:多人可领与单实例矛盾)。
func (u *MailUsecase) SendGuildMail(ctx context.Context, mailID, guildID uint64, title, body string, atts []*mailv1.MailAttachment, startMs, endMs, nowMs int64) (uint64, error) {
	if guildID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "guild_id required")
	}
	payload, err := u.buildPayload(title, body, atts, "", false)
	if err != nil {
		return 0, err
	}
	endMs = u.defaultEnd(startMs, endMs, nowMs)
	if endMs <= startMs {
		return 0, errcode.New(errcode.ErrInvalidArg,
			"mail window invalid: start=%d end=%d (lifetime capped at claim_retention_days=%d)",
			startMs, endMs, u.cfg.ClaimRetentionDays)
	}
	if err := u.repo.InsertGuildMail(ctx, mailID, guildID, startMs, endMs, payload); err != nil {
		return 0, err
	}
	return mailID, nil
}

// SendPersonalMail 写收件人收件箱(离线可达)。instanceGrantKey 非空时存入 payload,
// 领取 instance 形态附件走 GrantInstances 用此键去重(战斗掉落转邮件与直发共享源键 → 至多一次)。
// transfer 附件仅个人邮件可携带(收件人唯一,与托管行 to_player 一一对应;调用方须先
// EscrowOutInstances 托管,失败补偿 ReleaseTransferEscrow,bag-domain.md §7.1 saga)。
// expireMs=0 时补默认 TTL(一切邮件生命有限,sweep 清理的前提);写入侧上限见 InsertPersonalMail。
func (u *MailUsecase) SendPersonalMail(ctx context.Context, mailID, toPlayerID uint64, title, body string, atts []*mailv1.MailAttachment, expireMs, nowMs int64, instanceGrantKey string) (uint64, error) {
	if toPlayerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "to_player_id required")
	}
	payload, err := u.buildPayload(title, body, atts, instanceGrantKey, true)
	if err != nil {
		return 0, err
	}
	if expireMs == 0 {
		expireMs = nowMs + int64(u.cfg.DefaultPersonalTtlDays)*86400_000
	}
	if err := u.repo.InsertPersonalMail(ctx, mailID, toPlayerID, expireMs, payload, u.cfg.MaxInboxSize); err != nil {
		return 0, err
	}
	return mailID, nil
}

// partitionAttachments 把附件按 oneof 形态分成可堆叠(stack)、实例铸造(inst)、
// 托管转移(transfer)三组,各走独立发放/交付路径(transfer 绝不能落进 GrantInstances
// 铸造路径——那会把既存装备变成另一件东西)。unknown 计数由调用方 fail-closed 整封拒:
// body 未设置 / 真正未识别(滚更共存窗口旧端读到未来新增分支,§9.21)。
func partitionAttachments(atts []*mailv1.MailAttachment) (stack, inst, transfer []*mailv1.MailAttachment, unknown int) {
	for _, a := range atts {
		switch {
		case a.GetStack() != nil:
			stack = append(stack, a)
		case a.GetInstance() != nil:
			inst = append(inst, a)
		case a.GetTransfer() != nil:
			transfer = append(transfer, a)
		default:
			unknown++
		}
	}
	return stack, inst, transfer, unknown
}

// expandInstanceConfigIDs 把实例型附件按 count 逐件展开成配置 ID 列表(count 份 → count 个元素)。
// count=0 防御性视为 1 件(发送侧已校验 count>=1);供 inventory.GrantInstances 逐件铸造独立实例。
func expandInstanceConfigIDs(atts []*mailv1.MailAttachment) []uint32 {
	out := make([]uint32, 0, len(atts))
	for _, a := range atts {
		inst := a.GetInstance()
		if inst == nil {
			continue // 调用方已按 partitionAttachments 分组,此处只会收到 instance 形态
		}
		n := inst.GetCount()
		if n == 0 {
			n = 1
		}
		for i := uint32(0); i < n; i++ {
			out = append(out, inst.GetItemConfigId())
		}
	}
	return out
}

// defaultEnd 补默认有效期,并把 end_ms 钳到「创建时刻 + ClaimRetentionDays」以内。
//
// 领取记录按邮件创建时刻(雪花 mail_id)+ ClaimRetentionDays 清理(SweepExpired);
// 若邮件可领窗口超过该期限,claim 行会先于邮件消失,而 inventory 幂等流水自身只保留
// 90 天(CLAUDE.md §9.24),不能再当永久兜底 —— 超长邮件将可重复领奖。钳制后
// 「claim 行存活 ≥ 邮件可领窗口」恒成立,重复领取永远先被 claim 行挡住。
// 钳制基准用 nowMs(≈ mail_id 生成时刻)而非 startMs:定时邮件(start 在未来)的
// claim 清理 cutoff 仍按创建时刻算。
func (u *MailUsecase) defaultEnd(startMs, endMs, nowMs int64) int64 {
	base := startMs
	if base == 0 {
		base = nowMs
	}
	if endMs == 0 {
		endMs = base + int64(u.cfg.DefaultSysTtlDays)*dayMs
	}
	if maxEnd := nowMs + int64(u.cfg.ClaimRetentionDays)*dayMs; endMs > maxEnd {
		endMs = maxEnd
	}
	return endMs
}

func (u *MailUsecase) buildPayload(title, body string, atts []*mailv1.MailAttachment, instanceGrantKey string, allowTransfer bool) ([]byte, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return nil, errcode.New(errcode.ErrInvalidArg, "title required")
	}
	if utf8.RuneCountInString(title) > u.cfg.MaxTitleLen {
		return nil, errcode.New(errcode.ErrInvalidArg, "title too long")
	}
	if utf8.RuneCountInString(body) > u.cfg.MaxBodyLen {
		return nil, errcode.New(errcode.ErrInvalidArg, "body too long")
	}
	if len(atts) > u.cfg.MaxAttachments {
		return nil, errcode.New(errcode.ErrInvalidArg, "too many attachments")
	}
	// 发送侧校验附件形态:body 必须是已识别分支且 config_id/count 有效,
	// 拒绝空 body 入库(否则领取侧只能 fail-closed,邮件永远领不了)。
	seenInstances := map[uint64]bool{}
	var instanceTotal uint64 // instance 形态跨全部附件的累计件数,对齐 MaxInstancesPerMail
	for i, a := range atts {
		var cfgID, cnt uint32
		switch {
		case a.GetStack() != nil:
			cfgID, cnt = a.GetStack().GetItemConfigId(), a.GetStack().GetCount()
		case a.GetInstance() != nil:
			cfgID, cnt = a.GetInstance().GetItemConfigId(), a.GetInstance().GetCount()
		case a.GetTransfer() != nil:
			// transfer(既存实例托管转移,bag-domain.md §7.1)仅允许个人邮件携带:
			// 系统/公会邮件多人可领,与"单实例只改归属"矛盾(第一个领走后其余人整封
			// 领取失败)。调用方(受信内部服务)必须先经 inventory.EscrowOutInstances
			// 扣出托管再发信(saga);领取侧只认托管行,未托管先发 = 领取必失败。
			if !allowTransfer {
				return nil, errcode.New(errcode.ErrMailAttachmentUnsupported,
					"attachment[%d] transfer form only allowed in personal mail", i)
			}
			item := a.GetTransfer().GetItem()
			if item.GetInstanceId() == 0 || item.GetItemConfigId() == 0 || item.GetCount() != 1 {
				return nil, errcode.New(errcode.ErrInvalidArg,
					"attachment[%d] transfer requires instance_id/config and count=1", i)
			}
			if seenInstances[item.GetInstanceId()] {
				return nil, errcode.New(errcode.ErrInvalidArg,
					"attachment[%d] duplicate transfer instance %d", i, item.GetInstanceId())
			}
			seenInstances[item.GetInstanceId()] = true
			continue
		default:
			return nil, errcode.New(errcode.ErrMailAttachmentUnsupported, "attachment[%d] body required", i)
		}
		if cfgID == 0 || cnt == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "attachment[%d] item_config_id/count required", i)
		}
		// instance 形态的 count = 独立实例件数,领取时会按它循环铸 instance_id 并
		// append 进 intent。必须在**入库前**卡住累计上限:count 是 uint32,没有上界时
		// 一封邮件就能让 buildClaimIntent 的循环跑到内存耗尽(§9.18 写入侧上限 /
		// §16.5 容量边界)。发送侧拒掉 = 坏数据永不入库;领取侧另有一道同口径的
		// fail-closed 兜住存量行。
		if a.GetInstance() != nil {
			instanceTotal += uint64(cnt)
			if instanceTotal > uint64(u.cfg.MaxInstancesPerMail) {
				return nil, errcode.New(errcode.ErrInvalidArg,
					"attachment[%d] instance count exceeds per-mail limit: total=%d max=%d",
					i, instanceTotal, u.cfg.MaxInstancesPerMail)
			}
		}
	}
	rec := &mailv1.MailContentStorageRecord{Title: title, Body: body, Attachments: atts, InstanceGrantKey: instanceGrantKey}
	return proto.Marshal(rec)
}

func (u *MailUsecase) toChannelMail(ctx context.Context, playerID uint64, m data.MailRow, ch mailv1.MailChannel) *mailv1.Mail {
	claimed, _ := u.repo.HasClaimed(ctx, playerID, m.MailID)
	status := mailv1.MailStatus_MAIL_STATUS_READ // 拉取即视为已读
	if claimed {
		status = mailv1.MailStatus_MAIL_STATUS_CLAIMED
	}
	mail := decodePayload(m.Payload)
	mail.MailId = m.MailID
	mail.Channel = ch
	mail.Status = status
	mail.Claimed = claimed
	mail.CreatedMs = m.CreatedMs
	mail.ExpireMs = m.EndMs
	return mail
}

func toMail(m data.MailRow, ch mailv1.MailChannel, status int32, claimed bool) *mailv1.Mail {
	mail := decodePayload(m.Payload)
	mail.MailId = m.MailID
	mail.Channel = ch
	mail.Status = mailv1.MailStatus(status)
	mail.Claimed = claimed
	mail.CreatedMs = m.CreatedMs
	mail.ExpireMs = m.ExpireMs
	return mail
}

func decodePayload(payload []byte) *mailv1.Mail {
	rec := &mailv1.MailContentStorageRecord{}
	_ = proto.Unmarshal(payload, rec)
	return &mailv1.Mail{
		Title:       rec.GetTitle(),
		Body:        rec.GetBody(),
		Attachments: rec.GetAttachments(),
	}
}
