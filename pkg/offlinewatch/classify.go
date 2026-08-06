package offlinewatch

import "time"

// Verdict 是「这个玩家现在该不该按离线处理」的三态判定(外加一个不确定态)。
//
// ⚠️ 零值刻意是 VerdictUnknown 而不是 VerdictOffline:任何忘记赋值 / map 取不到 key /
// 结构体零值的路径,落到的都必须是「不动作」那一档。把「不确定」压成「离线」会让
// 依赖不可用时全服玩家被批量判离线(§9.22 明令禁止不确定冒充默认状态)。
type Verdict int

const (
	// VerdictUnknown 判定不了(缺少离开时刻基线 / 上游查询不可用)。调用方必须 fail-closed:
	// 不动作、推迟重试,绝不能当成离线。
	VerdictUnknown Verdict = iota
	// VerdictOnline 此刻在 locator 里查得到位置 —— 人在线(大厅 / 撮合 / 战斗都算)。
	VerdictOnline
	// VerdictWaiting 此刻查不到位置,但离开时间还没满阈值。继续等。
	VerdictWaiting
	// VerdictOffline 此刻查不到位置,且离开时间已满阈值。可以执行业务动作了。
	VerdictOffline
)

// String 便于日志与测试失败信息。
func (v Verdict) String() string {
	switch v {
	case VerdictOnline:
		return "online"
	case VerdictWaiting:
		return "waiting"
	case VerdictOffline:
		return "offline"
	default:
		return "unknown"
	}
}

// classify 是本包的全部判定逻辑,刻意做成无 I/O 的纯函数:离线判定一旦写错就是
// 「把在线玩家踢出队伍」这类不可逆后果,必须能被单测穷举覆盖,不能埋在网络调用中间。
//
// 入参语义(缺一不可,少任何一个都只能判 Unknown):
//   - online:  来自 locator BatchGetLocation。**在场即在线**,不区分 HUB/MATCHING/BATTLE
//     —— 玩家 travel 去战斗时位置是 MATCHING/BATTLE,那正是「不该按离线处理」的情形。
//   - lastSeen: 来自 locator BatchGetLastSeen,「最后一次被观测到离开 Hub 的时刻」。
//     缺席 = UNKNOWN,不是 0。
//   - hintMs:  调用方手里的旁证离开时刻(通常来自 kafka 事件的 left_at_ms)。
//     只在 lastSeen 缺席时兜底;<=0 表示没有旁证。
//
// 取 max(lastSeen, hint) 而不是任选其一:两者都是「离开时刻」的观测,取**更晚**的那个
// 意味着更晚才判超时 —— 偏保守的方向。反过来取更早的,会让一次「离开→回来→再离开」
// 里的旧时刻把新一轮在线期一笔勾销,直接判超时踢人。
func classify(
	nowMs int64,
	playerID uint64,
	online map[uint64]bool,
	lastSeen map[uint64]int64,
	hintMs int64,
	threshold time.Duration,
) (Verdict, int64) {
	if online[playerID] {
		return VerdictOnline, 0
	}

	sinceMs := hintMs
	if ms, ok := lastSeen[playerID]; ok && ms > sinceMs {
		sinceMs = ms
	}
	if sinceMs <= 0 {
		// 查不到位置,却也拿不到任何「什么时候离开的」基线。
		// 常见于:Hub DS 整台挂掉(压根没调 ReportDisconnect)、时刻已超保留期、
		// 或这个玩家本来就从没上过线。一律 UNKNOWN,交给调用方 fail-closed。
		return VerdictUnknown, 0
	}

	// 未来时刻(时钟回拨 / 上游写了个更晚的时间)按「刚离开」处理:elapsed 为负,
	// 自然落进 Waiting,不会提前触发。
	if nowMs-sinceMs >= threshold.Milliseconds() {
		return VerdictOffline, sinceMs
	}
	return VerdictWaiting, sinceMs
}
