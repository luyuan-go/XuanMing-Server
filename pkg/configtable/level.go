package configtable

import (
	"fmt"
	"strings"

	"github.com/luyuancpp/pandora/pkg/rating"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// 关卡表手写伴生文件:表私有校验钩子 + 域方法。
// 视图结构与通用访问 API(All/ByID/Exists/Count/ByIDs/RandOne/Where/First)在
// level_table.gen.go(tools/configtable-gen 生成,勿手改)。

// MaxLevelTeamSize 是关卡表「队伍人数」上限(团队级硬约束,防撮合预分配爆内存)。
// team_size 是裸 uint32、支持热更,撮合按 need=2*teamSize 预分配票据切片
// (matchmaker greedyFormMatches);若允许任意大值,热更一张超大 team_size 且该 map
// 有票即可触发巨量 make([]T,0,need) 直接打爆 matchmaker(§16.5 容量/溢出边界)。
// 基线是 5v5;取 50(单局最多 100 人)给未来大型玩法留 10x 余量,同时把预分配钳在可控量级。
// 0 是合法值(表示沿用服务端全局 team_size 兜底),只挡上界。
const MaxLevelTeamSize = 50

// validateLevelRow 逐行业务校验(生成的 newLevelTable 调用;主键非零/唯一已由生成代码兜住)。
// 与生成阶段校验重复是有意的 fail-closed:服务端不信任产物一定出自本生成器。
func validateLevelRow(row *configpb.LevelRow) error {
	if row.GetAssetPath() == "" {
		return fmt.Errorf("关卡资源(asset_path)为空")
	}
	if row.GetCategory() == configpb.LevelCategory_LEVEL_CATEGORY_UNSPECIFIED {
		return fmt.Errorf("关卡类别(category)未填")
	}
	// 上限校验:整表全批校验一处失败即拒新版本、保留旧表(§9.15 加载失败不切换),
	// 把「热更超大 team_size 打爆撮合」挡在加载边界,而不是等撮合时预分配爆内存。
	if ts := row.GetTeamSize(); ts > MaxLevelTeamSize {
		return fmt.Errorf("队伍人数(team_size=%d)超过上限 %d(防撮合预分配爆内存)", ts, MaxLevelTeamSize)
	}
	// 直进人数下限的两条硬约束(填了才校验,0=无下限是合法的默认态):
	//  ① 必须同时填上限。下限相对上限才有意义;上限留 0 表示"沿用服务端全局 team_size",
	//     而全局值逐部署不同,加载期无从比对——放过去等于让一张表在不同部署里下限时而合法
	//     时而大于上限(那会让该图任何人数都进不去,是个静默拒服务)。
	//  ② 不得大于上限。
	if minTS := row.GetMinTeamSize(); minTS > 0 {
		ts := row.GetTeamSize()
		if ts == 0 {
			return fmt.Errorf("队伍人数下限(min_team_size=%d)已填,但队伍人数(team_size)留空沿用全局兜底;两者必须同时填", minTS)
		}
		if minTS > ts {
			return fmt.Errorf("队伍人数下限(min_team_size=%d)大于队伍人数上限(team_size=%d)", minTS, ts)
		}
	}
	// 计分模式与对局结构的一致性:Elo 是「双方对抗的相对分」,单方合作副本没有对手结构,
	// 算给谁都说不通。填成 ELO 属于配置错配,挡在加载边界(整批不切换、保留旧表,§9.15),
	// 而不是等打完一局才在结算里给一群合作玩家互相扣分。
	// side_count==0 是"未配置沿用服务端默认 2 方",按 2 方对待,合法。
	if row.GetRatingMode() == configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO && row.GetSideCount() == 1 {
		return fmt.Errorf("计分模式(rating_mode)=ELO 但对局方数(side_count)=1(单方合作副本没有对手结构,无法算 Elo);要么改 1=不计分,要么把方数填成实际对抗方数")
	}
	// 段位池与计分模式必须配套,两向都拒(2026-08-11「3v3 与 5v5 不共用同一份段位」):
	//   - ELO 却没填池 → 结算时无处落账,只能兜底进 default 池,表现为"这张图的分和别的
	//     图混在一起算",而且**没有任何报错**——正是本次要消灭的那类静默错配;
	//   - 不计分却填了池 → 配置误解(填的人以为它生效了),早报早改。
	// 池名本身不设白名单(策划自由填,同值即同一份段位),只校验长度:超长会在非严格
	// sql_mode 下被静默截断成另一份段位(§9.24),必须挡在配置边界。
	pool := strings.TrimSpace(row.GetRatingPool())
	if row.GetRatingMode() == configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO {
		if pool == "" {
			return fmt.Errorf("计分模式(rating_mode)=ELO 但段位池(rating_pool)为空;必须指明本图算进哪一份段位(如 5v5_ranked / 3v3_ranked)")
		}
		if len(pool) > rating.MaxPoolLen {
			return fmt.Errorf("段位池(rating_pool=%q)长度 %d 超过上限 %d(超长会被数据库静默截断成另一份段位)", pool, len(pool), rating.MaxPoolLen)
		}
	} else if pool != "" {
		return fmt.Errorf("段位池(rating_pool=%q)已填,但计分模式(rating_mode)不是 2=按Elo算段位(本图根本不计分,填池无意义);要么把计分模式改成 2,要么清空本列", pool)
	}
	return nil
}

// LevelPackagePath 把关卡表「关卡资源」列(asset_path)归一成 UE **长包名**。
//
// 表里的值可能是 ObjectPath(`/Game/A/B.B`,导表从 SoftObjectPath 落下来就是这形状),
// 而 UE 的 ServerTravel / DS 命令行地图参数只吃包路径,带 `.对象名` 会解析失败。
// 规则与 UE 侧 APandoraDSLoaderGameMode::BuildTravelURL 逐字一致:剥掉**最后一个斜杠之后**
// 的那个点及其后缀(点在斜杠之前的路径原样返回,不误伤 `/Game/A.B/C` 这类目录名带点的情况)。
func LevelPackagePath(assetPath string) string {
	p := strings.TrimSpace(assetPath)
	if dot := strings.LastIndex(p, "."); dot > strings.LastIndex(p, "/") {
		p = p[:dot]
	}
	return p
}

// BattleLaunchURL 按 map_id 返回战斗 DS 要加载的关卡 URL(`<长包名>[?game=<GameMode 类>]`)。
//
// 这是「关卡表是唯一权威源」在服务端的落点:ds_allocator 起本机 DS 时按本函数拼命令行地图参数,
// 与 UE 侧 Loader GameMode(APandoraDSLoaderGameMode::BuildTravelURL)同规则、同数据源
// (g_关卡.xlsx),两端不会各拼一套。策划新增一张副本 = 关卡表加一行,服务端零改配置。
//
// **失败一律返回 error,绝不给兜底关卡**:调用方必须让本次分配整体失败。回退默认图会让 DS 起错图,
// 随后被 DS 侧关卡门判 Mismatch 自杀,表现成"玩家一直排队中"——2026-08-04 map_id=11 事故的形状。
//
// game_mode_class 为空 = 沿用关卡自带的 GameMode(不拼 `?game=`),与 UE 侧同义;
// 不在这里塞猜的默认 GameMode(要改就改表)。
func (t *LevelTable) BattleLaunchURL(id uint32) (string, error) {
	row, ok := t.byID[id]
	if !ok {
		return "", fmt.Errorf("关卡表(g_关卡.xlsx)没有 map_id=%d 的行", id)
	}
	if row.GetCategory() != configpb.LevelCategory_LEVEL_CATEGORY_BATTLE {
		return "", fmt.Errorf("map_id=%d(%s)关卡类别=%d,不是战斗 / 副本类关卡,不能开局",
			id, row.GetName(), row.GetCategory())
	}
	pkg := LevelPackagePath(row.GetAssetPath())
	if pkg == "" {
		return "", fmt.Errorf("map_id=%d(%s)关卡资源(asset_path)为空", id, row.GetName())
	}
	if gameMode := strings.TrimSpace(row.GetGameModeClass()); gameMode != "" {
		return pkg + "?game=" + gameMode, nil
	}
	return pkg, nil
}

// ValidateBattleLaunchURLs 断言表里**每一张**战斗类关卡都能构造出合法启动 URL。
// 供服务注册成批次级校验器(Store.AddValidator):坏批次整批不切换、保留旧表(§9.15),
// 把"某张图的资源列填错"挡在加载边界,而不是等玩家选中那张图才炸。
func (t *LevelTable) ValidateBattleLaunchURLs() error {
	for _, row := range t.rows {
		if row.GetCategory() != configpb.LevelCategory_LEVEL_CATEGORY_BATTLE {
			continue
		}
		if _, err := t.BattleLaunchURL(row.GetId()); err != nil {
			return err
		}
	}
	return nil
}

// IsBattleLevel 是否「存在且为战斗 / 副本类关卡」——匹配入口的 map_id 准入门:
// 只挡类别错误(登录 / 选角 / 主城不可开局),不看「匹配列表显示」列,
// 后者是客户端 UI 展示位,dev / GM 直进测试关卡(如 4 战斗测试)仍须放行。
func (t *LevelTable) IsBattleLevel(id uint32) bool {
	row, ok := t.byID[id]
	return ok && row.GetCategory() == configpb.LevelCategory_LEVEL_CATEGORY_BATTLE
}
