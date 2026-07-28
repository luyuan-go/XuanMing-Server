// mail_test.go — biz 层单测:实例型附件领取(instance 形态 → GrantInstances)+
// 溢出邮件源键透传(instance_grant_key 与直发链共享 → 至多一次)+
// 附件 oneof 形态校验(发送拒空 body,领取遇未识别形态 fail-closed 保持未领)。
package biz

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
	mailv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mail/v1"

	"github.com/luyuancpp/pandora/services/social/mail/internal/conf"
	"github.com/luyuancpp/pandora/services/social/mail/internal/data"
)

// stackAtt / instAtt 构造两种 oneof 形态附件(测试速记)。
func stackAtt(cfgID, count uint32) *mailv1.MailAttachment {
	return &mailv1.MailAttachment{Body: &mailv1.MailAttachment_Stack{
		Stack: &mailv1.StackAttachment{ItemConfigId: cfgID, Count: count},
	}}
}

func instAtt(cfgID, count uint32) *mailv1.MailAttachment {
	return &mailv1.MailAttachment{Body: &mailv1.MailAttachment_Instance{
		Instance: &mailv1.InstanceAttachment{ItemConfigId: cfgID, Count: count},
	}}
}

// ── 测试替身 ──────────────────────────────────────────────────────────────────

type storedMail struct {
	playerID uint64
	expireMs int64
	maxInbox int
	payload  []byte
}

// fakeMailRepo 是内存版 data.MailRepo,只落地个人邮件 + 领取幂等(其余方法返回零值);
// sweep 方法记录入参供断言(expired 预置待清行)。
type fakeMailRepo struct {
	personal map[uint64]storedMail // mailID → 邮件
	claimed  map[string]bool       // "player:mail" → 已领(终态)
	intents  map[string][]byte     // "player:mail" → DS 领取意图 payload(claimed=0)
	status   map[string]int32

	expired        []data.ExpiredPersonalRow // ListExpiredPersonal 返回值预置
	archivedRows   []data.ExpiredPersonalRow // ArchiveAndDeletePersonal 收到的归档行
	deletedIDs     []uint64                  // ArchiveAndDeletePersonal 收到的删除 ID
	sysEndBefore   int64
	guildEndBefore int64
	claimsBeforeID uint64
	purgeDays      int
	sysEnd         int64 // InsertSysMail 收到的 end_ms(断言 defaultEnd 钳制)
}

func newFakeMailRepo() *fakeMailRepo {
	return &fakeMailRepo{
		personal: map[uint64]storedMail{},
		claimed:  map[string]bool{},
		status:   map[string]int32{},
	}
}

func ck(playerID, mailID uint64) string {
	return itoa(playerID) + ":" + itoa(mailID)
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

func (r *fakeMailRepo) GetCursor(context.Context, uint64) (uint64, uint64, error) { return 0, 0, nil }
func (r *fakeMailRepo) GetPlayerGuild(context.Context, uint64) (uint64, bool, error) {
	return 0, false, nil
}
func (r *fakeMailRepo) ListPersonal(context.Context, uint64, int64, uint64, int) ([]data.MailRow, error) {
	return nil, nil
}
func (r *fakeMailRepo) ListSysSince(context.Context, uint64, int64) ([]data.MailRow, error) {
	return nil, nil
}
func (r *fakeMailRepo) ListGuildSince(context.Context, uint64, uint64, int64) ([]data.MailRow, error) {
	return nil, nil
}
func (r *fakeMailRepo) AdvanceCursor(context.Context, uint64, uint64, uint64) error { return nil }
func (r *fakeMailRepo) SetPersonalStatus(_ context.Context, playerID, mailID uint64, status int32) error {
	r.status[ck(playerID, mailID)] = status
	return nil
}
func (r *fakeMailRepo) DeletePersonal(context.Context, uint64, uint64) error { return nil }
func (r *fakeMailRepo) GetClaimablePayload(_ context.Context, playerID, mailID uint64, _ int64) ([]byte, bool, error) {
	m, ok := r.personal[mailID]
	if !ok || m.playerID != playerID {
		return nil, false, nil
	}
	return m.payload, true, nil
}
func (r *fakeMailRepo) HasClaimed(_ context.Context, playerID, mailID uint64) (bool, error) {
	return r.claimed[ck(playerID, mailID)], nil
}
func (r *fakeMailRepo) RecordClaim(_ context.Context, playerID, mailID uint64) (bool, error) {
	key := ck(playerID, mailID)
	if r.claimed[key] {
		return false, nil
	}
	r.claimed[key] = true
	return true, nil
}

// ── DS 三段式领取(bag phase 2)内存实现:intents 复刻 claimed=0 意图行 ──

func (r *fakeMailRepo) GetClaimState(_ context.Context, playerID, mailID uint64) (bool, bool, error) {
	key := ck(playerID, mailID)
	if r.claimed[key] {
		return true, false, nil
	}
	if r.intents != nil {
		if _, ok := r.intents[key]; ok {
			return false, true, nil
		}
	}
	return false, false, nil
}

func (r *fakeMailRepo) GetClaimIntent(_ context.Context, playerID, mailID uint64) ([]byte, bool, error) {
	if r.intents == nil {
		return nil, false, nil
	}
	blob, ok := r.intents[ck(playerID, mailID)]
	return blob, ok, nil
}

func (r *fakeMailRepo) CreateClaimIntent(_ context.Context, playerID, mailID uint64, payload []byte) (bool, error) {
	key := ck(playerID, mailID)
	if r.claimed[key] {
		return false, nil
	}
	if r.intents == nil {
		r.intents = map[string][]byte{}
	}
	if _, ok := r.intents[key]; ok {
		return false, nil
	}
	r.intents[key] = append([]byte(nil), payload...)
	return true, nil
}

func (r *fakeMailRepo) MarkClaimed(_ context.Context, playerID, mailID uint64) (bool, error) {
	key := ck(playerID, mailID)
	if r.claimed[key] {
		return true, nil
	}
	if r.intents != nil {
		if _, ok := r.intents[key]; ok {
			delete(r.intents, key)
			r.claimed[key] = true
			return true, nil
		}
	}
	return false, nil
}
func (r *fakeMailRepo) InsertSysMail(_ context.Context, _ uint64, _ int64, endMs int64, _ []byte) error {
	r.sysEnd = endMs
	return nil
}
func (r *fakeMailRepo) InsertGuildMail(context.Context, uint64, uint64, int64, int64, []byte) error {
	return nil
}
func (r *fakeMailRepo) InsertPersonalMail(_ context.Context, mailID, playerID uint64, expireMs int64, payload []byte, maxInbox int) error {
	r.personal[mailID] = storedMail{playerID: playerID, expireMs: expireMs, maxInbox: maxInbox, payload: payload}
	return nil
}

func (r *fakeMailRepo) ListExpiredPersonal(_ context.Context, _ int64, limit int) ([]data.ExpiredPersonalRow, error) {
	if len(r.expired) > limit {
		return r.expired[:limit], nil
	}
	return r.expired, nil
}

func (r *fakeMailRepo) ArchiveAndDeletePersonal(_ context.Context, archive []data.ExpiredPersonalRow, deleteIDs []uint64) error {
	r.archivedRows = append(r.archivedRows, archive...)
	r.deletedIDs = append(r.deletedIDs, deleteIDs...)
	return nil
}

func (r *fakeMailRepo) DeleteSysMailEndedBefore(_ context.Context, endBeforeMs int64, _ int) (int64, error) {
	r.sysEndBefore = endBeforeMs
	return 0, nil
}

func (r *fakeMailRepo) DeleteGuildMailEndedBefore(_ context.Context, endBeforeMs int64, _ int) (int64, error) {
	r.guildEndBefore = endBeforeMs
	return 0, nil
}

func (r *fakeMailRepo) DeleteClaimsBefore(_ context.Context, maxMailID uint64, _ int) (int64, error) {
	r.claimsBeforeID = maxMailID
	return 0, nil
}

func (r *fakeMailRepo) PurgeArchiveBefore(_ context.Context, retentionDays, _ int) (int64, error) {
	r.purgeDays = retentionDays
	return 0, nil
}

// fakeItemGranter 捕获可堆叠附件发放。
type fakeItemGranter struct {
	calls []itemCall
}
type itemCall struct {
	playerID uint64
	atts     []*mailv1.MailAttachment
	key      string
}

func (g *fakeItemGranter) Grant(_ context.Context, playerID uint64, atts []*mailv1.MailAttachment, key string) error {
	g.calls = append(g.calls, itemCall{playerID: playerID, atts: atts, key: key})
	return nil
}

// fakeInstGranter 捕获装备实例发放。
type fakeInstGranter struct {
	calls []instCall
}
type instCall struct {
	playerID uint64
	ids      []uint32
	key      string
}

func (g *fakeInstGranter) GrantInstances(_ context.Context, playerID uint64, ids []uint32, key string) error {
	g.calls = append(g.calls, instCall{playerID: playerID, ids: append([]uint32(nil), ids...), key: key})
	return nil
}

// fakeTransferClaimer 捕获 transfer 托管转移交付(可注入错误)。
type fakeTransferClaimer struct {
	calls []xferCall
	err   error
}
type xferCall struct {
	playerID uint64
	atts     []*mailv1.MailAttachment
	key      string
}

func (c *fakeTransferClaimer) ClaimTransfers(_ context.Context, playerID uint64, atts []*mailv1.MailAttachment, key string) error {
	c.calls = append(c.calls, xferCall{playerID: playerID, atts: atts, key: key})
	return c.err
}

// transferAtt 构造 transfer 形态附件(测试速记)。
func transferAtt(instanceID uint64, cfgID uint32) *mailv1.MailAttachment {
	return &mailv1.MailAttachment{Body: &mailv1.MailAttachment_Transfer{
		Transfer: &mailv1.TransferAttachment{
			Item: &bagv1.BagItem{ItemConfigId: cfgID, Count: 1, InstanceId: instanceID, Identified: true},
		},
	}}
}

func testCfg() conf.MailConf {
	return conf.MailConf{
		DefaultSysTtlDays: 7, DefaultPersonalTtlDays: 30, MaxInboxSize: 200,
		SweepBatch: 500, ExpiredRetentionDays: 7, ArchiveRetentionDays: 90, ClaimRetentionDays: 180,
		MaxTitleLen: 64, MaxBodyLen: 2048, MaxAttachments: 16,
	}
}

// ── 测试 ──────────────────────────────────────────────────────────────────────

// TestSendPersonalMailStoresGrantKey 发信时 instance_grant_key 落入 payload 存储记录。
func TestSendPersonalMailStoresGrantKey(t *testing.T) {
	repo := newFakeMailRepo()
	uc := NewMailUsecase(repo, testCfg(), &fakeItemGranter{})
	atts := []*mailv1.MailAttachment{instAtt(5001, 2)}
	id, err := uc.SendPersonalMail(context.Background(), 100, 1, "掉落", "背包已满", atts, 0, 1000, "battle_drop:9:1")
	if err != nil {
		t.Fatalf("SendPersonalMail err: %v", err)
	}
	rec := &mailv1.MailContentStorageRecord{}
	if err := proto.Unmarshal(repo.personal[id].payload, rec); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if rec.GetInstanceGrantKey() != "battle_drop:9:1" {
		t.Fatalf("grant key not stored, got %q", rec.GetInstanceGrantKey())
	}
}

// TestClaimInstanceUsesGrantKey 领取装备型附件走 GrantInstances,用 payload 里的源键去重(逐件展开)。
func TestClaimInstanceUsesGrantKey(t *testing.T) {
	repo := newFakeMailRepo()
	item := &fakeItemGranter{}
	inst := &fakeInstGranter{}
	uc := NewMailUsecase(repo, testCfg(), item)
	uc.SetInstanceGranter(inst)

	atts := []*mailv1.MailAttachment{instAtt(5001, 2)}
	id, err := uc.SendPersonalMail(context.Background(), 100, 1, "掉落", "背包已满", atts, 0, 1000, "battle_drop:9:1")
	if err != nil {
		t.Fatalf("SendPersonalMail err: %v", err)
	}
	if _, err := uc.ClaimMail(context.Background(), 1, id, 1000); err != nil {
		t.Fatalf("ClaimMail err: %v", err)
	}
	if len(inst.calls) != 1 {
		t.Fatalf("expected 1 GrantInstances call, got %d", len(inst.calls))
	}
	if inst.calls[0].key != "battle_drop:9:1" {
		t.Fatalf("instance grant key wrong: %s", inst.calls[0].key)
	}
	// count=2 → 逐件展开 [5001,5001]
	if len(inst.calls[0].ids) != 2 || inst.calls[0].ids[0] != 5001 || inst.calls[0].ids[1] != 5001 {
		t.Fatalf("instance ids expand wrong: %v", inst.calls[0].ids)
	}
	// 装备型附件不应走可堆叠 GrantItems。
	if len(item.calls) != 0 {
		t.Fatalf("instance attachment must not call item granter, got %d", len(item.calls))
	}
}

// TestClaimInstanceDefaultKey 无源键时装备型附件用默认键 mail_inst:{mail}:{player}。
func TestClaimInstanceDefaultKey(t *testing.T) {
	repo := newFakeMailRepo()
	inst := &fakeInstGranter{}
	uc := NewMailUsecase(repo, testCfg(), &fakeItemGranter{})
	uc.SetInstanceGranter(inst)

	atts := []*mailv1.MailAttachment{instAtt(5001, 1)}
	id, err := uc.SendPersonalMail(context.Background(), 200, 7, "装备", "运营发放", atts, 0, 1000, "")
	if err != nil {
		t.Fatalf("SendPersonalMail err: %v", err)
	}
	if _, err := uc.ClaimMail(context.Background(), 7, id, 1000); err != nil {
		t.Fatalf("ClaimMail err: %v", err)
	}
	if len(inst.calls) != 1 || inst.calls[0].key != "mail_inst:200:7" {
		t.Fatalf("default instance grant key wrong: %+v", inst.calls)
	}
}

// TestClaimStackableUsesItemGranter 可堆叠附件走 GrantItems,不碰 GrantInstances。
func TestClaimStackableUsesItemGranter(t *testing.T) {
	repo := newFakeMailRepo()
	item := &fakeItemGranter{}
	inst := &fakeInstGranter{}
	uc := NewMailUsecase(repo, testCfg(), item)
	uc.SetInstanceGranter(inst)

	atts := []*mailv1.MailAttachment{stackAtt(3001, 10)}
	id, err := uc.SendPersonalMail(context.Background(), 300, 5, "金币", "领奖", atts, 0, 1000, "")
	if err != nil {
		t.Fatalf("SendPersonalMail err: %v", err)
	}
	if _, err := uc.ClaimMail(context.Background(), 5, id, 1000); err != nil {
		t.Fatalf("ClaimMail err: %v", err)
	}
	if len(item.calls) != 1 || item.calls[0].key != "mail:300:5" {
		t.Fatalf("stackable must call item granter with mail key, got %+v", item.calls)
	}
	if len(inst.calls) != 0 {
		t.Fatalf("stackable must not call instance granter, got %d", len(inst.calls))
	}
}

// TestClaimMixedBothGranters 混合附件:可堆叠 + 装备实例分别走各自发放器。
func TestClaimMixedBothGranters(t *testing.T) {
	repo := newFakeMailRepo()
	item := &fakeItemGranter{}
	inst := &fakeInstGranter{}
	uc := NewMailUsecase(repo, testCfg(), item)
	uc.SetInstanceGranter(inst)

	atts := []*mailv1.MailAttachment{
		stackAtt(3001, 5), // 可堆叠
		instAtt(5001, 1),  // 实例型
	}
	id, err := uc.SendPersonalMail(context.Background(), 400, 8, "混合", "领奖", atts, 0, 1000, "battle_drop:1:8")
	if err != nil {
		t.Fatalf("SendPersonalMail err: %v", err)
	}
	if _, err := uc.ClaimMail(context.Background(), 8, id, 1000); err != nil {
		t.Fatalf("ClaimMail err: %v", err)
	}
	if len(item.calls) != 1 || len(item.calls[0].atts) != 1 || item.calls[0].atts[0].GetStack().GetItemConfigId() != 3001 {
		t.Fatalf("mixed: stackable routing wrong: %+v", item.calls)
	}
	if len(inst.calls) != 1 || len(inst.calls[0].ids) != 1 || inst.calls[0].ids[0] != 5001 {
		t.Fatalf("mixed: instance routing wrong: %+v", inst.calls)
	}
}

// TestClaimUnknownBodyFailClosed 领取遇到 oneof body 未识别的附件(滚更共存窗口旧端
// 读到未来新增形态)→ 整封 fail-closed:报 ErrMailAttachmentUnsupported、不发放任何
// 附件、不记 claim,邮件保持可领(升级后重领可成功),禁止静默跳过(§9.21)。
func TestClaimUnknownBodyFailClosed(t *testing.T) {
	repo := newFakeMailRepo()
	item := &fakeItemGranter{}
	inst := &fakeInstGranter{}
	uc := NewMailUsecase(repo, testCfg(), item)
	uc.SetInstanceGranter(inst)

	// 绕过发送侧校验直接落库:模拟新副本写入的"本端不认识的形态"
	//(未来分支在旧端反序列化后 oneof 未设置,等价于 body=nil)。
	payload, err := proto.Marshal(&mailv1.MailContentStorageRecord{
		Title: "未来形态", Body: "旧端不认识",
		Attachments: []*mailv1.MailAttachment{stackAtt(3001, 1), {}},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	const mailID, playerID = 500, 9
	repo.personal[mailID] = storedMail{playerID: playerID, payload: payload}

	_, err = uc.ClaimMail(context.Background(), playerID, mailID, 1000)
	var ec *errcode.Error
	if !errors.As(err, &ec) || ec.Code != errcode.ErrMailAttachmentUnsupported {
		t.Fatalf("want ErrMailAttachmentUnsupported, got %v", err)
	}
	// fail-closed:已识别的 stack 附件也不得先发(整封原子拒绝),不记 claim。
	if len(item.calls) != 0 || len(inst.calls) != 0 {
		t.Fatalf("fail-closed claim must not grant anything, item=%d inst=%d", len(item.calls), len(inst.calls))
	}
	if claimed, _ := repo.HasClaimed(context.Background(), playerID, mailID); claimed {
		t.Fatal("fail-closed claim must not record claim")
	}
}

// TestTransferPersonalMailClaimChain transfer 附件个人邮件全链(2026-07-22 接线,
// bag-domain.md §7.1):发送侧个人邮件放行 → 领取侧路由到 TransferClaimer
// (幂等键 mail_xfer:{mail}:{player}),混合附件各走各的交付器,claim 正常落记。
func TestTransferPersonalMailClaimChain(t *testing.T) {
	repo := newFakeMailRepo()
	item := &fakeItemGranter{}
	inst := &fakeInstGranter{}
	xfer := &fakeTransferClaimer{}
	uc := NewMailUsecase(repo, testCfg(), item)
	uc.SetInstanceGranter(inst)
	uc.SetTransferClaimer(xfer)

	atts := []*mailv1.MailAttachment{stackAtt(3001, 5), transferAtt(777, 5001)}
	id, err := uc.SendPersonalMail(context.Background(), 700, 3, "托管转移", "拍卖到账", atts, 0, 1000, "")
	if err != nil {
		t.Fatalf("send transfer personal mail: %v", err)
	}
	if _, err := uc.ClaimMail(context.Background(), 3, id, 1000); err != nil {
		t.Fatalf("ClaimMail err: %v", err)
	}
	if len(xfer.calls) != 1 || xfer.calls[0].playerID != 3 || xfer.calls[0].key != "mail_xfer:700:3" {
		t.Fatalf("transfer routing wrong: %+v", xfer.calls)
	}
	if len(xfer.calls[0].atts) != 1 || xfer.calls[0].atts[0].GetTransfer().GetItem().GetInstanceId() != 777 {
		t.Fatalf("transfer atts wrong: %+v", xfer.calls[0].atts)
	}
	// transfer 绝不落进 GrantInstances 铸造路径;stack 部分正常走 GrantItems。
	if len(inst.calls) != 0 {
		t.Fatalf("transfer must not call instance granter, got %d", len(inst.calls))
	}
	if len(item.calls) != 1 || item.calls[0].atts[0].GetStack().GetItemConfigId() != 3001 {
		t.Fatalf("mixed stack routing wrong: %+v", item.calls)
	}
	if claimed, _ := repo.HasClaimed(context.Background(), 3, id); !claimed {
		t.Fatal("claim should be recorded after successful transfer delivery")
	}
}

// TestTransferRejectedInSysGuildMail 系统/公会邮件拒收 transfer:多人可领与"单实例只改
// 归属"矛盾(第一个领走后其余人整封失败),发送侧 fail-closed。
func TestTransferRejectedInSysGuildMail(t *testing.T) {
	uc := NewMailUsecase(newFakeMailRepo(), testCfg(), &fakeItemGranter{})
	atts := []*mailv1.MailAttachment{transferAtt(777, 5001)}

	var ec *errcode.Error
	_, err := uc.SendSystemMail(context.Background(), 701, "sys", "x", atts, 0, 0, 1000)
	if !errors.As(err, &ec) || ec.Code != errcode.ErrMailAttachmentUnsupported {
		t.Fatalf("sys transfer: want ErrMailAttachmentUnsupported, got %v", err)
	}
	_, err = uc.SendGuildMail(context.Background(), 702, 88, "guild", "x", atts, 0, 0, 1000)
	if !errors.As(err, &ec) || ec.Code != errcode.ErrMailAttachmentUnsupported {
		t.Fatalf("guild transfer: want ErrMailAttachmentUnsupported, got %v", err)
	}
}

// TestTransferSendShapeValidation 发送侧 transfer 形状校验:instance_id/config 必填、
// count 恒 1、同封不允许重复实例。
func TestTransferSendShapeValidation(t *testing.T) {
	uc := NewMailUsecase(newFakeMailRepo(), testCfg(), &fakeItemGranter{})
	var ec *errcode.Error

	bad := &mailv1.MailAttachment{Body: &mailv1.MailAttachment_Transfer{
		Transfer: &mailv1.TransferAttachment{Item: &bagv1.BagItem{ItemConfigId: 5001, Count: 1}}, // instance_id=0
	}}
	_, err := uc.SendPersonalMail(context.Background(), 703, 3, "坏形状", "x", []*mailv1.MailAttachment{bad}, 0, 1000, "")
	if !errors.As(err, &ec) || ec.Code != errcode.ErrInvalidArg {
		t.Fatalf("zero instance: want ErrInvalidArg, got %v", err)
	}

	bad2 := &mailv1.MailAttachment{Body: &mailv1.MailAttachment_Transfer{
		Transfer: &mailv1.TransferAttachment{Item: &bagv1.BagItem{ItemConfigId: 5001, Count: 2, InstanceId: 777}},
	}}
	_, err = uc.SendPersonalMail(context.Background(), 704, 3, "坏count", "x", []*mailv1.MailAttachment{bad2}, 0, 1000, "")
	if !errors.As(err, &ec) || ec.Code != errcode.ErrInvalidArg {
		t.Fatalf("count!=1: want ErrInvalidArg, got %v", err)
	}

	_, err = uc.SendPersonalMail(context.Background(), 705, 3, "重复实例", "x",
		[]*mailv1.MailAttachment{transferAtt(777, 5001), transferAtt(777, 5001)}, 0, 1000, "")
	if !errors.As(err, &ec) || ec.Code != errcode.ErrInvalidArg {
		t.Fatalf("dup instance: want ErrInvalidArg, got %v", err)
	}
}

// TestTransferClaimerUnavailableFailClosed 未注入 TransferClaimer 时 transfer 领取严格拒
// (AllowNoopGrant 也不放行:空领 = 邮件标已领而托管行滞留,实例资产静默丢失),
// 不记 claim,邮件保持可领。claimer 报错时同样保持未领取。
func TestTransferClaimerUnavailableFailClosed(t *testing.T) {
	repo := newFakeMailRepo()
	cfg := testCfg()
	cfg.AllowNoopGrant = true // 即便测试空领配置打开,transfer 也不放行
	uc := NewMailUsecase(repo, cfg, &fakeItemGranter{})

	payload, merr := proto.Marshal(&mailv1.MailContentStorageRecord{
		Title: "托管转移", Body: "x",
		Attachments: []*mailv1.MailAttachment{transferAtt(777, 5001)},
	})
	if merr != nil {
		t.Fatalf("marshal payload: %v", merr)
	}
	const mailID, playerID = 501, 9
	repo.personal[mailID] = storedMail{playerID: playerID, payload: payload}

	_, err := uc.ClaimMail(context.Background(), playerID, mailID, 1000)
	var ec *errcode.Error
	if !errors.As(err, &ec) || ec.Code != errcode.ErrInternal {
		t.Fatalf("claim without claimer: want ErrInternal, got %v", err)
	}
	if claimed, _ := repo.HasClaimed(context.Background(), playerID, mailID); claimed {
		t.Fatal("fail-closed claim must not record claim")
	}

	// claimer 注入但交付失败(inventory 拒/不可达)→ 不记 claim,重领可重试。
	xfer := &fakeTransferClaimer{err: errcode.New(errcode.ErrInventoryItemNotFound, "escrow missing")}
	uc.SetTransferClaimer(xfer)
	if _, err := uc.ClaimMail(context.Background(), playerID, mailID, 1000); err == nil {
		t.Fatal("claimer error must fail the claim")
	}
	if claimed, _ := repo.HasClaimed(context.Background(), playerID, mailID); claimed {
		t.Fatal("failed transfer delivery must not record claim")
	}
}

// TestSendRejectsInvalidAttachment 发送侧校验:body 未设置拒收(否则该邮件永远领不了);
// config_id/count 为零拒收。
func TestSendRejectsInvalidAttachment(t *testing.T) {
	uc := NewMailUsecase(newFakeMailRepo(), testCfg(), &fakeItemGranter{})

	_, err := uc.SendPersonalMail(context.Background(), 600, 3, "空body", "x",
		[]*mailv1.MailAttachment{{}}, 0, 1000, "")
	var ec *errcode.Error
	if !errors.As(err, &ec) || ec.Code != errcode.ErrMailAttachmentUnsupported {
		t.Fatalf("empty body: want ErrMailAttachmentUnsupported, got %v", err)
	}

	_, err = uc.SendPersonalMail(context.Background(), 601, 3, "零count", "x",
		[]*mailv1.MailAttachment{stackAtt(3001, 0)}, 0, 1000, "")
	if !errors.As(err, &ec) || ec.Code != errcode.ErrInvalidArg {
		t.Fatalf("zero count: want ErrInvalidArg, got %v", err)
	}
	_, err = uc.SendPersonalMail(context.Background(), 602, 3, "零config", "x",
		[]*mailv1.MailAttachment{instAtt(0, 1)}, 0, 1000, "")
	if !errors.As(err, &ec) || ec.Code != errcode.ErrInvalidArg {
		t.Fatalf("zero config: want ErrInvalidArg, got %v", err)
	}
}

// ── DS 三段式领取(bag phase 2)────────────────────────────────────────────────

// fakeIDGen 递增实例 ID 生成器(意图展开铸 ID 用)。
type fakeIDGen struct{ next uint64 }

func (g *fakeIDGen) Generate() uint64 { g.next++; return g.next }

// GenerateInto 与真实 snowflake.Node 同语义:逐个取,保证严格递增且唯一。
func (g *fakeIDGen) GenerateInto(dst []uint64) {
	for i := range dst {
		dst[i] = g.Generate()
	}
}

// fakeEscrowConsumer 捕获托管消费调用。
type fakeEscrowConsumer struct {
	calls [][]uint64
	err   error
}

func (c *fakeEscrowConsumer) ConsumeTransferEscrow(_ context.Context, _ uint64, ids []uint64) error {
	c.calls = append(c.calls, append([]uint64(nil), ids...))
	return c.err
}

// TestDSClaimIntentStableAcrossReplay 意图展开一次落库:instance 形态铸 ID 一次,
// 重取返回同一批 ID(bag journal 指纹去重的前提);transfer 原样透传并记入消费清单。
func TestDSClaimIntentStableAcrossReplay(t *testing.T) {
	repo := newFakeMailRepo()
	uc := NewMailUsecase(repo, testCfg(), &fakeItemGranter{})
	uc.SetInstanceIDGen(&fakeIDGen{next: 100})

	atts := []*mailv1.MailAttachment{stackAtt(3001, 5), instAtt(5001, 2), transferAtt(777, 6001)}
	id, err := uc.SendPersonalMail(context.Background(), 800, 3, "混合", "x", atts, 0, 1000, "")
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	items, key, already, err := uc.GetClaimableAttachments(context.Background(), 3, id, 1000)
	if err != nil || already {
		t.Fatalf("get intent: already=%v err=%v", already, err)
	}
	if key != "mail_claim:800:3" {
		t.Fatalf("claim key wrong: %s", key)
	}
	// stack 1 条 + instance 2 条(铸 ID 101/102)+ transfer 1 条(原样 777)。
	if len(items) != 4 {
		t.Fatalf("expand wrong: %d items", len(items))
	}
	if items[1].GetInstanceId() != 101 || items[2].GetInstanceId() != 102 {
		t.Fatalf("instance mint wrong: %d/%d", items[1].GetInstanceId(), items[2].GetInstanceId())
	}
	if items[3].GetInstanceId() != 777 || !items[3].GetIdentified() {
		t.Fatalf("transfer passthrough wrong: %+v", items[3])
	}

	// 重取(崩溃重放):逐字节同内容,不重铸 ID。
	items2, _, already2, err := uc.GetClaimableAttachments(context.Background(), 3, id, 1000)
	if err != nil || already2 {
		t.Fatalf("replay: already=%v err=%v", already2, err)
	}
	if len(items2) != 4 || items2[1].GetInstanceId() != 101 || items2[2].GetInstanceId() != 102 {
		t.Fatalf("replay must return identical expansion: %+v", items2)
	}

	// 意图创建后旧直连 ClaimMail 互斥拒(9607),不发放不记 claim。
	_, cerr := uc.ClaimMail(context.Background(), 3, id, 1000)
	var ec *errcode.Error
	if !errors.As(cerr, &ec) || ec.Code != errcode.ErrMailClaimInProgress {
		t.Fatalf("legacy claim on open intent: want 9607, got %v", cerr)
	}
}

// TestDSClaimMarkConsumesEscrowThenFinalizes Mark:先消 transfer 托管行再置终态;
// 终态后 Get 返回 already_claimed,Mark 幂等 no-op,消费不重复。
func TestDSClaimMarkConsumesEscrowThenFinalizes(t *testing.T) {
	repo := newFakeMailRepo()
	uc := NewMailUsecase(repo, testCfg(), &fakeItemGranter{})
	uc.SetInstanceIDGen(&fakeIDGen{})
	consumer := &fakeEscrowConsumer{}
	uc.SetTransferEscrowConsumer(consumer)

	atts := []*mailv1.MailAttachment{transferAtt(777, 6001)}
	id, err := uc.SendPersonalMail(context.Background(), 801, 3, "转移", "x", atts, 0, 1000, "")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, _, _, err := uc.GetClaimableAttachments(context.Background(), 3, id, 1000); err != nil {
		t.Fatalf("get intent: %v", err)
	}
	if err := uc.MarkMailClaimed(context.Background(), 3, id); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if len(consumer.calls) != 1 || len(consumer.calls[0]) != 1 || consumer.calls[0][0] != 777 {
		t.Fatalf("escrow consume wrong: %+v", consumer.calls)
	}
	// 终态:Get → already;Mark 幂等;消费不重复。
	_, _, already, err := uc.GetClaimableAttachments(context.Background(), 3, id, 1000)
	if err != nil || !already {
		t.Fatalf("after mark: already=%v err=%v", already, err)
	}
	if err := uc.MarkMailClaimed(context.Background(), 3, id); err != nil {
		t.Fatalf("mark replay: %v", err)
	}
	if len(consumer.calls) != 1 {
		t.Fatalf("mark replay must not re-consume: %d", len(consumer.calls))
	}
	if st := repo.status[ck(3, id)]; st != data.StatusClaimed {
		t.Fatalf("personal status not claimed: %d", st)
	}
}

// TestDSClaimMarkFailClosed Mark 时序违规与依赖缺失 fail-closed:
// 无意图直接 Mark 拒;托管消费失败不置终态(重 Mark 重消,恰好一次)。
func TestDSClaimMarkFailClosed(t *testing.T) {
	repo := newFakeMailRepo()
	uc := NewMailUsecase(repo, testCfg(), &fakeItemGranter{})
	uc.SetInstanceIDGen(&fakeIDGen{})

	atts := []*mailv1.MailAttachment{transferAtt(778, 6001)}
	id, err := uc.SendPersonalMail(context.Background(), 802, 3, "转移", "x", atts, 0, 1000, "")
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// 无意图直接 Mark → ErrInvalidArg(journal 前不得终结)。
	var ec *errcode.Error
	if merr := uc.MarkMailClaimed(context.Background(), 3, id); !errors.As(merr, &ec) || ec.Code != errcode.ErrInvalidArg {
		t.Fatalf("mark without intent: want ErrInvalidArg, got %v", merr)
	}

	if _, _, _, gerr := uc.GetClaimableAttachments(context.Background(), 3, id, 1000); gerr != nil {
		t.Fatalf("get intent: %v", gerr)
	}
	// 含 transfer 但未注入消费器 → 拒终结(防托管行残留双持)。
	if merr := uc.MarkMailClaimed(context.Background(), 3, id); !errors.As(merr, &ec) || ec.Code != errcode.ErrInternal {
		t.Fatalf("mark without consumer: want ErrInternal, got %v", merr)
	}
	// 消费失败 → 不置终态,重 Mark 重消。
	consumer := &fakeEscrowConsumer{err: errcode.New(errcode.ErrUnavailable, "inventory down")}
	uc.SetTransferEscrowConsumer(consumer)
	if merr := uc.MarkMailClaimed(context.Background(), 3, id); errcode.As(merr) != errcode.ErrUnavailable {
		t.Fatalf("consume failure must propagate: %v", merr)
	}
	consumer.err = nil
	if merr := uc.MarkMailClaimed(context.Background(), 3, id); merr != nil {
		t.Fatalf("mark retry: %v", merr)
	}
	if len(consumer.calls) != 2 {
		t.Fatalf("expect consume retried: %d", len(consumer.calls))
	}
}

// TestDSClaimInstanceRequiresIDGen 含 instance 形态附件且未注入 ID 生成器 → 意图拒创建。
func TestDSClaimInstanceRequiresIDGen(t *testing.T) {
	repo := newFakeMailRepo()
	uc := NewMailUsecase(repo, testCfg(), &fakeItemGranter{})

	id, err := uc.SendPersonalMail(context.Background(), 803, 3, "装备", "x",
		[]*mailv1.MailAttachment{instAtt(5001, 1)}, 0, 1000, "")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, _, _, gerr := uc.GetClaimableAttachments(context.Background(), 3, id, 1000); errcode.As(gerr) != errcode.ErrInternal {
		t.Fatalf("want ErrInternal without id gen, got %v", gerr)
	}
}

// ── 实例件数上限(§9.18 写入侧上限 / §16.5 容量边界)──────────────────────────

// TestSendMail_RejectsInstanceCountOverLimit 发送侧必须在**入库前**拒掉超限的实例件数:
// count 是 uint32,没有上界时 buildClaimIntent 会按它循环铸 ID 并 append,内存先被吃光。
func TestSendMail_RejectsInstanceCountOverLimit(t *testing.T) {
	cfg := testCfg()
	cfg.MaxInstancesPerMail = 10
	uc := NewMailUsecase(newFakeMailRepo(), cfg, &fakeItemGranter{})
	uc.SetInstanceIDGen(&fakeIDGen{})

	// 单个附件超限。
	atts := []*mailv1.MailAttachment{instAtt(5001, 11)}
	if _, err := uc.SendPersonalMail(context.Background(), 100, 1, "t", "b", atts, 0, 1000, ""); err == nil {
		t.Fatal("单附件超限必须拒绝")
	}

	// 多个附件各自不超限、累计超限 —— 只卡单附件挡不住这种。
	atts = []*mailv1.MailAttachment{instAtt(5001, 6), instAtt(5002, 6)}
	if _, err := uc.SendPersonalMail(context.Background(), 100, 1, "t", "b", atts, 0, 1000, ""); err == nil {
		t.Fatal("跨附件累计超限必须拒绝")
	}

	// 边界:恰好等于上限必须放行。
	atts = []*mailv1.MailAttachment{instAtt(5001, 4), instAtt(5002, 6)}
	if _, err := uc.SendPersonalMail(context.Background(), 100, 1, "t", "b", atts, 0, 1000, ""); err != nil {
		t.Fatalf("恰好等于上限应放行: %v", err)
	}
}

// TestNewMailUsecase_ZeroLimitFallsBackToDefault 钉死零值语义:
// 上限漏配必须退回默认值,而不是变成「拒绝一切实例附件」把正常业务静默打死(§14.2)。
func TestNewMailUsecase_ZeroLimitFallsBackToDefault(t *testing.T) {
	cfg := testCfg()
	cfg.MaxInstancesPerMail = 0 // 漏配
	uc := NewMailUsecase(newFakeMailRepo(), cfg, &fakeItemGranter{})
	uc.SetInstanceIDGen(&fakeIDGen{})

	if uc.cfg.MaxInstancesPerMail != conf.DefaultMaxInstancesPerMail {
		t.Fatalf("零值应归一化为 %d, got %d",
			conf.DefaultMaxInstancesPerMail, uc.cfg.MaxInstancesPerMail)
	}
	atts := []*mailv1.MailAttachment{instAtt(5001, 2)}
	if _, err := uc.SendPersonalMail(context.Background(), 100, 1, "t", "b", atts, 0, 1000, ""); err != nil {
		t.Fatalf("漏配上限不应拒绝正常邮件: %v", err)
	}
}
