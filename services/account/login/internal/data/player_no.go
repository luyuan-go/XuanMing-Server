// player_no.go — 角色编号(player_no)补号任务的数据层。
// 设计与拍板见 docs/design/player-no-and-login-surge.md §3(2026-08-10 落码)。
//
// 形态:注册路径零参与(CreateAccount 不碰编号),本文件的批量补号事务把 accounts 里
// player_no IS NULL 的行按 (created_at, player_id) 全序编号。要点:
//   - 全局互斥:事务先 FOR UPDATE 锁 player_no_counter 单行(id=1)。多 login 副本
//     并发调用被该行锁天然串行化,无需 leader election(同 social 库 guild counter 先例);
//   - 事务隔离必须 READ COMMITTED(2026-08-10 真 TiDB 并发测试修正):计数器行锁只串行化
//     「写」,取批 SELECT 的可见性由隔离级别决定。TiDB 悲观事务在 RR 下用 BeginTx 时刻的
//     start_ts 快照服务普通 SELECT——后到的 sweeper 在计数器锁上等前一批提交后,取批扫描
//     仍看不到刚提交的编号,会重扫同一批行,复核 UPDATE(当前读)恒 affected=0,把正常并发
//     误判成"第二写者"整批回滚(InnoDB RR 恰好不炸只是因为 read view 迟到首个一致性读才建,
//     在 FOR UPDATE 返回之后)。RC 下两端都是逐语句新快照:扫描发生在拿锁之后,必然包含
//     锁前驱批次的提交。真 TiDB 回归:player_no_mysql_test.go ConcurrentSweepersSerialize;
//   - 无空洞:取批、编号、推进计数器在同一事务,回滚即整体消失,编号严格连续;
//   - 不锁账号行:player_no 只有本事务(被计数器锁串行化)写,行级 FOR UPDATE 冗余,
//     且 InnoDB 下扫 uk_player_no 的 NULL 范围会产生间隙锁、反向阻塞并发注册 INSERT
//     (RC 顺带免除该间隙锁),故意不加;以 UPDATE ... AND player_no IS NULL +
//     RowsAffected==1 复核兜底(§16.1),复核失败说明存在计数器锁之外的第二写者,
//     整批回滚 fail-closed,绝不发出可疑号;
//   - 水位安全滞后:只处理 created_at < NOW() - lag 的行,封死「打戳早、提交可见晚」的
//     迟可见错序(细节与 10s 推导见设计文档 §3.3)。比较用 NOW() 而非 UTC_TIMESTAMP():
//     created_at 由列 DEFAULT CURRENT_TIMESTAMP 按会话时区打戳,水位必须同一时区口径,
//     换 UTC_TIMESTAMP() 在服务器非 UTC 时区时水位会整体漂移。
//
// 红线(设计文档 §3.3):player_no 是纯展示字段,任何服务不得作身份键/外键/路由键/幂等键。
//
// 语义归属(§3.6.1,2026-08-10 用户拍板):编号绑定**角色实体**而非账号——项目角色可交易
// (卖角色/过户),编号是角色的资历凭证(编号小=老角色,是定价要素),过户时随角色走、值不变;
// 一账号建 N 个角色即有 N 个编号。今天 player_id 一身兼两职(账号身份+角色身份),故本文件
// 按 player_id 编号即已正确;多角色改造时 player_id 下沉为角色实体 ID,player_no 随之
// 落到角色表,本文件只需把扫描目标从 accounts 换成角色表(编号逻辑与红线均不变)。
// ⚠️ 与红线不冲突:"不得作身份键"约束的是**别的服务不能拿它当键**,不妨碍它是角色的固有展示属性。
//
// 删角色不回收编号(§3.6.2,2026-08-10 用户拍板)——**本文件是该保证的唯一承载处**:
// 保证来自「`next_no` 只增不减」(下方推进计数器只有 `next_no + N`,无任何回退路径),
// 故物理上不可能重发已发过的号;与删除逻辑无关(角色行物理删还是软删都不重号)。
// 理由:编号是历史事实,回收会让两个角色先后持同一编号——卖角色业务里是欺诈载体
// (新角色可冒充已注销老号的资历),且交易纠纷追溯失去唯一锚点。
// **运维红线**:禁止任何「重置计数器」「按存活角色回填编号」的操作(会直接制造重号);
// 计数器只允许由本文件的补号事务推进。改本文件前先读 §3.6.2。
package data

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

const (
	// PlayerNoBatchSize 单事务编号行数上限(对齐仓库批处理 LIMIT 500 惯例,控制事务大小)。
	PlayerNoBatchSize = 500

	// PlayerNoWatermarkLag 水位安全滞后:INSERT 打戳(语句执行)到行可见(提交)存在间隙,
	// 补号只处理打戳超过该滞后的行,保证任何行在被编号前至少有整个滞后窗口可见,
	// 从而编号全序与 created_at 全序严格一致。取值 = 语句/gRPC 超时上界(prod 5s)×2,
	// 保守偏大,待实测复核(§16.10.③);代价只是编号可见延迟 +10s,展示场景无感。
	PlayerNoWatermarkLag = 10 * time.Second
)

// EnsurePlayerNoCounter 启动期探针 + 双代计数器幂等初始化。
//
// 探针:SELECT player_no 验证库已收敛到 000006 的目标列;未迁移的存量库在此一次性失败,
// 调用方停用补号任务(展示功能 fail-soft,不拦 login 启动——对比 sql_mode 断言那类
// 才有资格 fail-fast,§9.24)。
// 初始化:INSERT IGNORE 单行 id=1;计数器已存在时不改 next_no,即起始号(拍板项 A③)
// 只在首次初始化生效,之后改配置无效(编号连续性不允许事后改起点)。
func EnsurePlayerNoCounter(ctx context.Context, db *sql.DB, startNo uint64) error {
	if startNo == 0 {
		startNo = 1
	}
	var playerProbe, legacyProbe sql.NullInt64
	err := db.QueryRowContext(ctx, "SELECT player_no, register_no FROM accounts LIMIT 1").Scan(&playerProbe, &legacyProbe)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return errcode.New(errcode.ErrInternal, "player_no 双列探针失败(pandora_account 000007 expand 未完成?): %v", err)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return errcode.New(errcode.ErrInternal, "player_no ensure begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO player_no_counter (id, next_no) VALUES (1, ?)", startNo); err != nil {
		return errcode.New(errcode.ErrInternal, "init player_no_counter: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO register_no_counter (id, next_no) VALUES (1, ?)", startNo); err != nil {
		return errcode.New(errcode.ErrInternal, "init register_no_counter: %v", err)
	}
	var playerNext, legacyNext uint64
	if err := tx.QueryRowContext(ctx, "SELECT next_no FROM player_no_counter WHERE id=1 FOR UPDATE").Scan(&playerNext); err != nil {
		return errcode.New(errcode.ErrInternal, "lock player_no_counter: %v", err)
	}
	if err := tx.QueryRowContext(ctx, "SELECT next_no FROM register_no_counter WHERE id=1 FOR UPDATE").Scan(&legacyNext); err != nil {
		return errcode.New(errcode.ErrInternal, "lock register_no_counter: %v", err)
	}
	next := playerNext
	if legacyNext > next {
		next = legacyNext
	}
	if _, err := tx.ExecContext(ctx, "UPDATE player_no_counter SET next_no=? WHERE id=1", next); err != nil {
		return errcode.New(errcode.ErrInternal, "sync player_no_counter: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE register_no_counter SET next_no=? WHERE id=1", next); err != nil {
		return errcode.New(errcode.ErrInternal, "sync register_no_counter: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return errcode.New(errcode.ErrInternal, "player_no ensure commit: %v", err)
	}
	return nil
}

// SweepPlayerNo 执行一批补号,返回本批实际编号的行数。
// 返回 0 表示当前无待编号行(或全部仍在水位滞后窗口内),调用方据此结束本轮 drain。
// 多副本并发调用安全(计数器行锁串行化);任何错误整批回滚,无空洞、无半批。
func SweepPlayerNo(ctx context.Context, db *sql.DB, batch int) (int, error) {
	if batch <= 0 {
		batch = PlayerNoBatchSize
	}
	lagSec := int(PlayerNoWatermarkLag / time.Second)

	// READ COMMITTED 是正确性要求,不是调优:取批 SELECT 必须看到计数器锁前驱批次的提交,
	// TiDB 悲观事务 RR 的 start_ts 快照做不到(见头注释;真 TiDB 并发回归钉在测试③)。
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "player_no sweep begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var playerNext, legacyNext uint64
	if err := tx.QueryRowContext(ctx,
		"SELECT next_no FROM player_no_counter WHERE id = 1 FOR UPDATE").Scan(&playerNext); err != nil {
		return 0, errcode.New(errcode.ErrInternal,
			"player_no counter lock(计数器未初始化? 见 EnsurePlayerNoCounter): %v", err)
	}
	if err := tx.QueryRowContext(ctx,
		"SELECT next_no FROM register_no_counter WHERE id = 1 FOR UPDATE").Scan(&legacyNext); err != nil {
		return 0, errcode.New(errcode.ErrInternal, "register_no compatibility counter lock: %v", err)
	}
	next := playerNext
	if legacyNext > next {
		next = legacyNext
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT player_id, COALESCE(player_no, register_no, 0),
		        player_no IS NOT NULL, register_no IS NOT NULL
		   FROM accounts
		 WHERE (player_no IS NULL OR register_no IS NULL) AND created_at < NOW() - INTERVAL ? SECOND
		 ORDER BY created_at, player_id LIMIT ?`, lagSec, batch)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "player_no select pending: %v", err)
	}
	type pendingPlayerNo struct {
		playerID  uint64
		existing  uint64
		playerHas bool
		legacyHas bool
	}
	pending := make([]pendingPlayerNo, 0, batch)
	for rows.Next() {
		var row pendingPlayerNo
		if err := rows.Scan(&row.playerID, &row.existing, &row.playerHas, &row.legacyHas); err != nil {
			_ = rows.Close()
			return 0, errcode.New(errcode.ErrInternal, "player_no scan pending: %v", err)
		}
		pending = append(pending, row)
	}
	if err := rows.Close(); err != nil {
		return 0, errcode.New(errcode.ErrInternal, "player_no close pending: %v", err)
	}
	if err := rows.Err(); err != nil {
		return 0, errcode.New(errcode.ErrInternal, "player_no iterate pending: %v", err)
	}
	if len(pending) == 0 {
		return 0, nil // 空转:defer Rollback 收尾,只读事务无副作用
	}

	newAssignments := uint64(0)
	for _, row := range pending {
		assigned := row.existing
		if !row.playerHas && !row.legacyHas {
			assigned = next + newAssignments
			newAssignments++
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE accounts SET player_no = ?, register_no = ?
			 WHERE player_id = ?
			   AND (player_no IS NULL OR player_no = ?)
			   AND (register_no IS NULL OR register_no = ?)
			   AND (player_no IS NULL OR register_no IS NULL)`,
			assigned, assigned, row.playerID, assigned, assigned)
		if err != nil {
			return 0, errcode.New(errcode.ErrInternal, "player_no assign player_id=%d: %v", row.playerID, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, errcode.New(errcode.ErrInternal, "player_no assign affected player_id=%d: %v", row.playerID, err)
		}
		if n != 1 {
			return 0, errcode.New(errcode.ErrInternal,
				"player_no assign player_id=%d affected=%d(计数器锁外存在第二写者或双列冲突,整批回滚)", row.playerID, n)
		}
	}

	newNext := next + newAssignments
	if _, err := tx.ExecContext(ctx,
		"UPDATE player_no_counter SET next_no = ? WHERE id = 1", newNext); err != nil {
		return 0, errcode.New(errcode.ErrInternal, "player_no advance counter: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		"UPDATE register_no_counter SET next_no = ? WHERE id = 1", newNext); err != nil {
		return 0, errcode.New(errcode.ErrInternal, "register_no compatibility advance counter: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, errcode.New(errcode.ErrInternal, "player_no sweep commit: %v", err)
	}
	return len(pending), nil
}
