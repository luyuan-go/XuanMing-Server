// skill_card_repo_mysql_test.go — 技能卡发放 / 培养 / 装配的真实 MySQL 集成回归(2026-08-11 补)。
//
// 这三条路径的正确性都不在 Go 代码里,而在引擎的加锁与约束语义上,fake repo 验不到:
//   - GrantSkillCards 的幂等靠 `skill_card_grants.uk_player_key` 撞键,
//     且「重复获得转碎片」是 `ON DUPLICATE KEY UPDATE shards = shards + VALUES(shards)`
//     —— **level 刻意不在 UPDATE 子句里**,发放不得重置已培养等级(代码注释的这条承诺
//     只有真库能验:换成先查后写的实现在单测里照样绿,在真并发下丢更新);
//   - UpgradeSkillCard 的「读余量 → 判够不够 → 扣」三步靠 `FOR UPDATE` 串行化(§16.1 TOCTOU),
//     没有真行锁时两次并发升级能用同一批碎片各升一级;
//   - SetSkillSlots 的最后一道防线是 `uk_player_card_once`(同一张卡不得占两个槽),
//     代码注释明写"库是最后一道",那就必须真的拿库来验。
package data

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

const skillCardTestTimeout = 20 * time.Second

// TestGrantSkillCardsIdempotency_MySQL —— 同一发放幂等键重放:一张卡一片碎片都不加。
func TestGrantSkillCardsIdempotency_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), skillCardTestTimeout)
	defer cancel()

	const playerID = uint64(71_001)
	seedPlayerProfile(t, repo, playerID, 1500)

	grants := []SkillCardGrant{{CardID: 1001, Shards: 10}, {CardID: 1002, Shards: 0}}
	cards, already, err := repo.GrantSkillCards(ctx, playerID, grants, "gacha-71001-1")
	if err != nil {
		t.Fatalf("首次发放: %v", err)
	}
	if already {
		t.Fatal("首次发放不应命中幂等")
	}
	if len(cards) != 2 {
		t.Fatalf("首次发放回读 %d 张卡, want 2", len(cards))
	}
	// 获得即 1 级;碎片为 0 的那张只解锁不给碎片。
	for _, c := range cards {
		if c.Level != 1 {
			t.Fatalf("卡 %d level=%d, want 1(获得即 1 级)", c.CardID, c.Level)
		}
	}

	replay, replayAlready, err := repo.GrantSkillCards(ctx, playerID, grants, "gacha-71001-1")
	if err != nil {
		t.Fatalf("重放发放: %v", err)
	}
	if !replayAlready {
		t.Fatal("同一 idempotency_key 重放必须命中幂等(uk_player_key 没起作用)")
	}
	if len(replay) != 2 {
		t.Fatalf("幂等回读 %d 张卡, want 2", len(replay))
	}
	for _, c := range replay {
		want := uint32(0)
		if c.CardID == 1001 {
			want = 10
		}
		if c.Shards != want {
			t.Fatalf("重放后卡 %d shards=%d, want %d(幂等分支不得再累加碎片)", c.CardID, c.Shards, want)
		}
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM skill_card_grants WHERE player_id = ?`, playerID); got != 1 {
		t.Fatalf("skill_card_grants 行数=%d, want 1", got)
	}
}

// TestGrantSkillCardsRegrantKeepsCultivatedLevel_MySQL —— 重复获得同名卡只转碎片,
// **绝不能把已培养到的等级冲回 1 级**(玩家资产回退,且无法从数据里还原)。
func TestGrantSkillCardsRegrantKeepsCultivatedLevel_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), skillCardTestTimeout)
	defer cancel()

	const (
		playerID = uint64(71_002)
		cardID   = uint32(1001)
	)
	seedPlayerProfile(t, repo, playerID, 1500)

	if _, _, err := repo.GrantSkillCards(ctx, playerID,
		[]SkillCardGrant{{CardID: cardID, Shards: 10}}, "gacha-71002-1"); err != nil {
		t.Fatalf("首次发放: %v", err)
	}
	// 先把卡培养到 2 级,把碎片花光。
	upgraded, spent, err := repo.UpgradeSkillCard(ctx, playerID, cardID, map[uint32]uint32{2: 10}, 5)
	if err != nil {
		t.Fatalf("升级到 2 级: %v", err)
	}
	if upgraded.Level != 2 || upgraded.Shards != 0 || spent != 10 {
		t.Fatalf("升级结果 level=%d shards=%d spent=%d, want 2/0/10", upgraded.Level, upgraded.Shards, spent)
	}

	// 再次获得同一张卡:只加碎片。
	after, already, err := repo.GrantSkillCards(ctx, playerID,
		[]SkillCardGrant{{CardID: cardID, Shards: 7}}, "gacha-71002-2")
	if err != nil {
		t.Fatalf("二次发放: %v", err)
	}
	if already {
		t.Fatal("换了幂等键不应命中幂等")
	}
	if len(after) != 1 {
		t.Fatalf("回读 %d 张卡, want 1", len(after))
	}
	if after[0].Level != 2 {
		t.Fatalf("二次发放后 level=%d, want 2(发放不得重置培养等级)", after[0].Level)
	}
	if after[0].Shards != 7 {
		t.Fatalf("二次发放后 shards=%d, want 7(碎片应累加到升级后的 0 上)", after[0].Shards)
	}
}

// TestUpgradeSkillCardConcurrentCannotDoubleSpend_MySQL —— §16.1 TOCTOU:
// 只够升一级的碎片,并发多次升级只能成功一次。
func TestUpgradeSkillCardConcurrentCannotDoubleSpend_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), skillCardTestTimeout)
	defer cancel()

	const (
		playerID   = uint64(71_003)
		cardID     = uint32(1001)
		concurrent = 6
	)
	seedPlayerProfile(t, repo, playerID, 1500)
	if _, _, err := repo.GrantSkillCards(ctx, playerID,
		[]SkillCardGrant{{CardID: cardID, Shards: 10}}, "gacha-71003-1"); err != nil {
		t.Fatalf("发放: %v", err)
	}

	// 每一级都要 10 片,而玩家只有 10 片 —— 正好只够升一级。
	costByLevel := map[uint32]uint32{2: 10, 3: 10, 4: 10, 5: 10}
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded int
		spentSum  uint32
		otherErr  error
	)
	start := make(chan struct{})
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, spent, err := repo.UpgradeSkillCard(ctx, playerID, cardID, costByLevel, 5)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded++
				spentSum += spent
			case errcode.As(err) == errcode.ErrSkillCardInsufficientShards:
				// 预期的拒绝
			default:
				otherErr = err
			}
		}()
	}
	close(start)
	wg.Wait()

	if otherErr != nil {
		t.Fatalf("并发升级出现非预期错误: %v", otherErr)
	}
	if succeeded != 1 {
		t.Fatalf("并发升级成功 %d 次, want 1(FOR UPDATE 未串行化,同一批碎片被花了多次)", succeeded)
	}
	if spentSum != 10 {
		t.Fatalf("累计扣除 %d 片, want 10", spentSum)
	}
	cards, err := repo.GetSkillCards(ctx, playerID)
	if err != nil {
		t.Fatalf("回读持卡: %v", err)
	}
	if len(cards) != 1 || cards[0].Level != 2 || cards[0].Shards != 0 {
		t.Fatalf("落库 %+v, want level=2 shards=0", cards)
	}
}

// TestUpgradeSkillCardRejections_MySQL —— 三条拒绝路径必须都不写库。
func TestUpgradeSkillCardRejections_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), skillCardTestTimeout)
	defer cancel()

	const (
		playerID = uint64(71_004)
		cardID   = uint32(1001)
	)
	seedPlayerProfile(t, repo, playerID, 1500)

	// 未持有
	if _, _, err := repo.UpgradeSkillCard(ctx, playerID, cardID, map[uint32]uint32{2: 1}, 5); errcode.As(err) != errcode.ErrSkillCardNotOwned {
		t.Fatalf("未持有升级 err=%v, want ErrSkillCardNotOwned", err)
	}

	if _, _, err := repo.GrantSkillCards(ctx, playerID,
		[]SkillCardGrant{{CardID: cardID, Shards: 100}}, "gacha-71004-1"); err != nil {
		t.Fatalf("发放: %v", err)
	}

	// 已达上限:maxLevel=1 而当前就是 1 级。
	if _, _, err := repo.UpgradeSkillCard(ctx, playerID, cardID, map[uint32]uint32{2: 1}, 1); errcode.As(err) != errcode.ErrSkillCardMaxLevel {
		t.Fatalf("满级升级 err=%v, want ErrSkillCardMaxLevel", err)
	}

	// 曲线断档:上限允许升,但目标等级没有价钱。绝不能当免费升级放行。
	if _, _, err := repo.UpgradeSkillCard(ctx, playerID, cardID, map[uint32]uint32{3: 1}, 5); errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("曲线断档升级 err=%v, want ErrInternal(不得白升)", err)
	}

	cards, err := repo.GetSkillCards(ctx, playerID)
	if err != nil {
		t.Fatalf("回读持卡: %v", err)
	}
	if len(cards) != 1 || cards[0].Level != 1 || cards[0].Shards != 100 {
		t.Fatalf("拒绝路径改动了数据: %+v, want level=1 shards=100", cards)
	}
}

// TestSetSkillSlotsConcurrentNeverPlacesOneCardTwice_MySQL —— `uk_player_card_once`
// 是同一张卡占两个槽的最后一道防线(repo 注释原话)。并发两次全量替换交错时,
// 终态必须仍然是"每张卡至多占一个槽",失败方拿到 ErrSkillCardSlotInvalid 而不是脏数据。
func TestSetSkillSlotsConcurrentNeverPlacesOneCardTwice_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), skillCardTestTimeout)
	defer cancel()

	const playerID = uint64(71_005)
	seedPlayerProfile(t, repo, playerID, 1500)
	if _, _, err := repo.GrantSkillCards(ctx, playerID, []SkillCardGrant{
		{CardID: 1001, Shards: 0}, {CardID: 1002, Shards: 0},
	}, "gacha-71005-1"); err != nil {
		t.Fatalf("发放: %v", err)
	}

	// 两个写者把同一张卡装到不同槽:任何交错下都不允许两行同时留存。
	layouts := [][]SkillSlot{
		{{Slot: 0, CardID: 1001}, {Slot: 1, CardID: 1002}},
		{{Slot: 2, CardID: 1001}, {Slot: 3, CardID: 1002}},
	}
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		otherErr error
	)
	start := make(chan struct{})
	for _, layout := range layouts {
		layout := layout
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := repo.SetSkillSlots(ctx, playerID, layout)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
			case errcode.As(err) == errcode.ErrSkillCardSlotInvalid:
				// 交错撞 uk,预期的拒绝
			case errcode.As(err) == errcode.ErrInternal:
				// InnoDB 可能把交错的 DELETE+INSERT 判成死锁并回滚其中一方;
				// 事务整体回滚同样保住了不变量,所以不算失败,但要保证终态仍然自洽。
			default:
				otherErr = err
			}
		}()
	}
	close(start)
	wg.Wait()

	if otherErr != nil {
		t.Fatalf("并发装配出现非预期错误: %v", otherErr)
	}
	slots, err := repo.GetSkillSlots(ctx, playerID)
	if err != nil {
		t.Fatalf("回读卡槽: %v", err)
	}
	seen := make(map[uint32]uint32, len(slots))
	for _, s := range slots {
		if prev, dup := seen[s.CardID]; dup {
			t.Fatalf("卡 %d 同时占了槽 %d 与 %d(uk_player_card_once 未兜住)", s.CardID, prev, s.Slot)
		}
		seen[s.CardID] = s.Slot
	}
}
