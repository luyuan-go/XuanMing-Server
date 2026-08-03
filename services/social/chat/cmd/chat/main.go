// Pandora chat 服务入口(2026-06-16)。
//
// 职责:世界 / 队伍 / 私聊三频道聊天;私聊落 pandora_social(MySQL 强依赖,离线历史);
// 三频道经 kafka pandora.chat.{world,team,private} → push 推送(弱依赖);
// 队伍频道成员经 team 服务 gRPC 解析(弱依赖,addr 空则降级)。
//
// 启动顺序(对齐 friend / team):
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. MySQL client(强依赖:私聊历史落库不可降级)
//  5. Snowflake Node(message_id 生成,node_id 来自 yaml)
//  6. kafka 五 producer(chat.private/team/world/guild/group)→ chatPusher(弱依赖)
//  7. team / guild gRPC client → TeamReader / GuildReader / GroupReader(弱依赖,addr 空则降级)
//  8. 装配 ChatUsecase → ChatService → gRPC/HTTP server
//  9. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	klog "github.com/go-kratos/kratos/v2/log"

	"github.com/luyuancpp/pandora/pkg/cellroute/etcdtable"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"
	chatv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/chat/v1"

	"github.com/luyuancpp/pandora/services/social/chat/internal/biz"
	"github.com/luyuancpp/pandora/services/social/chat/internal/conf"
	"github.com/luyuancpp/pandora/services/social/chat/internal/data"
	"github.com/luyuancpp/pandora/services/social/chat/internal/server"
	"github.com/luyuancpp/pandora/services/social/chat/internal/service"
)

const serviceName = "chat"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/chat-dev.yaml", "config file path")
}

func main() {
	flag.Parse()

	// 1. Logger
	logger := plog.Setup(serviceName)
	helper := plog.NewHelper(logger)
	helper.Infow("msg", "service_starting", "conf", flagConf)

	// 2. 加载 yaml
	cfgPath, err := filepath.Abs(flagConf)
	if err != nil {
		helper.Errorw("msg", "abs_conf_path_failed", "err", err)
		os.Exit(1)
	}
	c := kconfig.New(kconfig.WithSource(file.NewSource(cfgPath)))
	defer func() { _ = c.Close() }()

	if err := c.Load(); err != nil {
		helper.Errorw("msg", "config_load_failed", "err", err, "path", cfgPath)
		os.Exit(1)
	}

	var cfg conf.Config
	if err := c.Scan(&cfg); err != nil {
		helper.Errorw("msg", "config_scan_failed", "err", err)
		os.Exit(1)
	}
	cfg.Defaults()
	// 保留期清理模式必须能被识别(§9.24 fail-fast):拼错的值会静默回落 report_only,
	// 运维以为开了清理、实际一行没删,库继续无界增长且启动期毫无痕迹。
	if err := cfg.Chat.ValidateRetentionMode(); err != nil {
		helper.Errorw("msg", "chat_retention_mode_invalid", "err", err,
			"hint", "chat.retention_mode 只接受 \"report_only\"(默认,不删) 或 \"delete\"")
		os.Exit(1)
	}

	// 3. MySQL(强依赖:私聊历史落库不可降级)
	if cfg.Node.MySQLClient.DSN == "" {
		helper.Errorw("msg", "mysql_dsn_required", "hint", "node.mysql_client.dsn required (pandora_social)")
		os.Exit(1)
	}
	db := mysqlx.MustNewClient(cfg.Node.MySQLClient)
	defer func() { _ = db.Close() }()
	helper.Infow("msg", "mysql_connected", "dsn", maskDSN(cfg.Node.MySQLClient.DSN))

	// 严格模式断言(§9.24):非严格 sql_mode 下超长写入会被 MySQL **静默截断**
	// (err=nil 但数据被砍断),等于无声的数据损坏,故 fail-fast 而不是继续产生坏数据。
	if serr := dbguard.AssertStrictModeStartup(db); serr != nil {
		helper.Errorw("msg", "mysql_strict_mode_required", "err", serr)
		os.Exit(1)
	}

	// 容量巡检(§9.24):启动即跑一轮拿基线(上线时就已超限当场可见),之后每小时一轮。
	// 走 information_schema 估算(毫秒级不锁表);超预算只打 ERROR 日志 + metric,不阻止启动。
	capacityCtx, capacityCancel := context.WithCancel(context.Background())
	defer capacityCancel()
	go runCapacityGuard(capacityCtx, dbguard.New(db, "pandora_social", data.Budgets(), nil))

	// 4. Snowflake(message_id 生成；node_id_source=static 静态，=etcd 走 etcd 自动抢占，失租自动退出)
	sf, sfCloser := etcdnode.MustProvideSnowflake(serviceName, cfg.Node.NodeId, cfg.Snowflake)
	defer func() { _ = sfCloser.Close() }()

	// 5. kafka 三 producer → chatPusher(弱依赖:任一 producer 初始化失败则整体降级,聊天推送静默 fail)
	var pusher biz.ChatPusher
	if len(cfg.Kafka.Brokers) > 0 {
		if cp := newChatPusher(cfg, helper); cp != nil {
			defer cp.Close()
			pusher = cp
		}
	} else {
		helper.Warnw("msg", "kafka_brokers_empty", "hint", "chat push disabled (private still persisted)")
	}

	// 6. team gRPC client → TeamReader(弱依赖:addr 空则队伍频道降级)
	var teamReader biz.TeamReader
	if cfg.Chat.TeamAddr != "" {
		tr := data.NewGrpcTeamReader(cfg.Chat.TeamAddr)
		defer func() { _ = tr.Close() }()
		teamReader = tr
		helper.Infow("msg", "team_client_ready", "team_addr", cfg.Chat.TeamAddr)
	} else {
		helper.Warnw("msg", "team_addr_empty", "hint", "team channel fan-out disabled")
	}

	// 6b. guild gRPC client → GuildReader / GroupReader(弱依赖:addr 空则公会 / 群频道降级)。
	// GuildService + GroupService 同进程,共用 guild_addr;两个 reader 各自拨连。
	var guildReader biz.GuildReader
	var groupReader biz.GroupReader
	if cfg.Chat.GuildAddr != "" {
		gr := data.NewGrpcGuildReader(cfg.Chat.GuildAddr)
		defer func() { _ = gr.Close() }()
		guildReader = gr
		gpr := data.NewGrpcGroupReader(cfg.Chat.GuildAddr)
		defer func() { _ = gpr.Close() }()
		groupReader = gpr
		helper.Infow("msg", "guild_client_ready", "guild_addr", cfg.Chat.GuildAddr)
	} else {
		helper.Warnw("msg", "guild_addr_empty", "hint", "guild / group channel fan-out disabled")
	}

	// 7. 装配链
	repo := data.NewMySQLPrivateRepo(db)
	uc := biz.NewChatUsecase(repo, pusher, teamReader, guildReader, groupReader, cfg.Chat)

	// 世界频道 per-player 冷却(压测审核【必修-5】):复用 node.redis_client(与会话门同一
	// Redis)。未配 Redis 的骨架联调不限流(Warn 提示);生产 prod 生成器已强制配 Redis。
	if cfg.Node.RedisClient.Host != "" {
		rdb := redisx.NewClient(cfg.Node.RedisClient)
		defer func() { _ = rdb.Close() }()
		uc.SetWorldRateLimiter(data.NewRedisWorldRateLimiter(rdb))
		helper.Infow("msg", "world_ratelimit_ready", "cooldown", cfg.Chat.WorldCooldown.Std().String())
	} else {
		helper.Warnw("msg", "world_ratelimit_disabled", "hint", "node.redis_client.host empty, world channel unthrottled")
	}
	if closeCell, e := etcdtable.WireRouter(context.Background(), cfg.CellRoute, uc.SetCellRouter); e != nil {
		helper.Errorw("msg", "cellroute_init_failed", "err", e)
		os.Exit(1)
	} else if closeCell != nil {
		defer func() { _ = closeCell() }()
	}
	svc := service.NewChatService(uc, sf)

	// 私聊历史保留期清理:按雪花 message_id cutoff 批删超期行,只增表增长有界
	// (§9.24,biz/sweep.go)。多副本各自跑,DELETE 幂等无需锁。
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	defer sweepCancel()
	go uc.RunHistorySweep(sweepCtx)

	// 会话现行性门(R5 复审 P0-1,INC-20260722-004):客户端面请求 jti 必须是 login
	// 会话权威(pandora:sess,node.redis_client 指向的共享 Redis)当前一代;
	// prod 生成器机械置 session_gate.require=true(漏配端点拒启)。
	sessGate, sgClose := sessiongate.MustBuild(cfg.Node.RedisClient, cfg.SessionGate.Require)
	defer sgClose()

	grpcSrv := server.NewGRPCServer(&cfg, svc, sessGate)
	httpSrv := server.NewHTTPServer(&cfg)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"kafka_brokers", cfg.Kafka.Brokers,
		"team_addr", cfg.Chat.TeamAddr,
		"guild_addr", cfg.Chat.GuildAddr,
		"max_content_len", cfg.Chat.MaxContentLen,
	)

	// 8. Kratos App
	app := kratos.New(
		kratos.Name(serviceName),
		kratos.Logger(logger),
		kratos.Server(grpcSrv, httpSrv),
	)
	if err := app.Run(); err != nil {
		helper.Errorw("msg", "app_run_failed", "err", err)
		os.Exit(1)
	}
}

// chatPusher 把 biz.ChatPusher 接口适配到五个 kafkax.KeyOrderedProducer。
// kafka key:私聊 / 队伍 / 公会 / 群 = 收件方 player_id(同接收方保序);世界频道广播 key 空。
type chatPusher struct {
	private *kafkax.KeyOrderedProducer
	team    *kafkax.KeyOrderedProducer
	world   *kafkax.KeyOrderedProducer
	guild   *kafkax.KeyOrderedProducer
	group   *kafkax.KeyOrderedProducer
}

// newChatPusher 初始化五 producer;任一失败则关闭已建的并返回 nil(整体降级)。
func newChatPusher(cfg conf.Config, helper *klog.Helper) *chatPusher {
	priv, err := kafkax.NewKeyOrderedProducer(cfg.Kafka, kafkax.TopicChatPrivate)
	if err != nil {
		helper.Warnw("msg", "kafka_producer_init_failed", "topic", kafkax.TopicChatPrivate, "err", err)
		return nil
	}
	team, err := kafkax.NewKeyOrderedProducer(cfg.Kafka, kafkax.TopicChatTeam)
	if err != nil {
		helper.Warnw("msg", "kafka_producer_init_failed", "topic", kafkax.TopicChatTeam, "err", err)
		_ = priv.Close()
		return nil
	}
	world, err := kafkax.NewKeyOrderedProducer(cfg.Kafka, kafkax.TopicChatWorld)
	if err != nil {
		helper.Warnw("msg", "kafka_producer_init_failed", "topic", kafkax.TopicChatWorld, "err", err)
		_ = priv.Close()
		_ = team.Close()
		return nil
	}
	guild, err := kafkax.NewKeyOrderedProducer(cfg.Kafka, kafkax.TopicChatGuild)
	if err != nil {
		helper.Warnw("msg", "kafka_producer_init_failed", "topic", kafkax.TopicChatGuild, "err", err)
		_ = priv.Close()
		_ = team.Close()
		_ = world.Close()
		return nil
	}
	group, err := kafkax.NewKeyOrderedProducer(cfg.Kafka, kafkax.TopicChatGroup)
	if err != nil {
		helper.Warnw("msg", "kafka_producer_init_failed", "topic", kafkax.TopicChatGroup, "err", err)
		_ = priv.Close()
		_ = team.Close()
		_ = world.Close()
		_ = guild.Close()
		return nil
	}
	helper.Infow("msg", "kafka_producer_ready", "topics", []string{
		kafkax.TopicChatPrivate, kafkax.TopicChatTeam, kafkax.TopicChatWorld,
		kafkax.TopicChatGuild, kafkax.TopicChatGroup,
	})
	return &chatPusher{private: priv, team: team, world: world, guild: guild, group: group}
}

func (p *chatPusher) PushPrivate(ctx context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error {
	return p.private.Send(ctx, strconv.FormatUint(toPlayerID, 10), evt)
}

func (p *chatPusher) PushTeam(ctx context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error {
	return p.team.Send(ctx, strconv.FormatUint(toPlayerID, 10), evt)
}

func (p *chatPusher) PushWorld(ctx context.Context, evt *chatv1.ChatPushEvent) error {
	// 世界频道广播:key 空,push 服务侧 Broadcast 路由给全体。
	return p.world.Send(ctx, "", evt)
}

func (p *chatPusher) PushGuild(ctx context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error {
	return p.guild.Send(ctx, strconv.FormatUint(toPlayerID, 10), evt)
}

func (p *chatPusher) PushGroup(ctx context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error {
	return p.group.Send(ctx, strconv.FormatUint(toPlayerID, 10), evt)
}

func (p *chatPusher) Close() {
	if p.private != nil {
		_ = p.private.Close()
	}
	if p.team != nil {
		_ = p.team.Close()
	}
	if p.world != nil {
		_ = p.world.Close()
	}
	if p.guild != nil {
		_ = p.guild.Close()
	}
	if p.group != nil {
		_ = p.group.Close()
	}
}

// runCapacityGuard 启动即跑一轮容量巡检拿基线,之后每小时一轮(§9.24)。
// 走 information_schema 估算(毫秒级、不锁表、不扫数据),放启动路径安全;
// 绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。超预算只告警不阻断。
func runCapacityGuard(ctx context.Context, g *dbguard.Guard) {
	safego.Run(ctx, "db_capacity_guard_initial", func() { g.Check(ctx) })
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(ctx, "db_capacity_guard", func() { g.Check(ctx) })
		}
	}
}

// maskDSN 脱敏 DSN 里的密码(对齐 friend / player main.go)。
func maskDSN(dsn string) string {
	at := -1
	colon := -1
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == ':' && colon == -1 {
			colon = i
		}
		if dsn[i] == '@' {
			at = i
			break
		}
	}
	if colon != -1 && at != -1 && at > colon {
		return dsn[:colon+1] + "***" + dsn[at:]
	}
	return dsn
}
