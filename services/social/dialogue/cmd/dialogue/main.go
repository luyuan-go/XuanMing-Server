// Pandora dialogue 服务入口(2026-06-16)。
//
// 职责:NPC 对话树运行时;StartDialogue / ChooseOption / EndDialogue 三个 unary RPC。
//   - 对话树从 yaml 配置加载(MOBA 早期:简单 if-else,不上行为树)。
//   - 会话状态(dialogue_id)由服务端持有,当前为单实例内存会话(MemorySessionStore)。
//
// 阶段限制:内存会话不跨实例、进程重启即丢。多实例部署需把 SessionStore 换 Redis 版
// (biz / service 不动)。当前对话选项无副作用(领奖励 / 改任务等留后续接 trade / player)。
//
// 启动顺序(对齐 friend / team):
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. Snowflake Node(dialogue_id 生成,node_id 来自 yaml)
//  5. 配置对话树 → ConfigTreeProvider;内存会话 → MemorySessionStore
//  6. 装配 DialogueUsecase → DialogueService → gRPC/HTTP server
//  7. 启动会话过期清理 goroutine
//  8. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	klog "github.com/go-kratos/kratos/v2/log"

	"github.com/luyuancpp/pandora/pkg/cellroute/etcdtable"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"

	"github.com/luyuancpp/pandora/services/social/dialogue/internal/biz"
	"github.com/luyuancpp/pandora/services/social/dialogue/internal/conf"
	"github.com/luyuancpp/pandora/services/social/dialogue/internal/data"
	"github.com/luyuancpp/pandora/services/social/dialogue/internal/server"
	"github.com/luyuancpp/pandora/services/social/dialogue/internal/service"
)

const serviceName = "dialogue"

// sessionSweepInterval 是会话过期清理 goroutine 的扫描周期。
const sessionSweepInterval = time.Minute

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/dialogue-dev.yaml", "config file path")
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

	// 3. Snowflake(dialogue_id 生成；node_id_source=static 静态，=etcd 走 etcd 自动抢占，失租自动退出)
	sf, sfCloser := etcdnode.MustProvideSnowflake(serviceName, cfg.Node.NodeId, cfg.Snowflake)
	defer func() { _ = sfCloser.Close() }()

	// 4. 对话树:配置 → ConfigTreeProvider(构造时做基本校验,起始节点缺失直接 fatal)
	trees, err := buildTrees(cfg.Dialogue.Trees)
	if err != nil {
		helper.Errorw("msg", "dialogue_tree_invalid", "err", err)
		os.Exit(1)
	}
	treeProvider := data.NewConfigTreeProvider(trees)

	// 5. 内存会话存储
	sessions := data.NewMemorySessionStore()

	// 6. 装配链
	uc := biz.NewDialogueUsecase(treeProvider, sessions, cfg.Dialogue.SessionTTL.Std())
	if closeCell, e := etcdtable.WireRouter(context.Background(), cfg.CellRoute, uc.SetCellRouter); e != nil {
		helper.Errorw("msg", "cellroute_init_failed", "err", e)
		os.Exit(1)
	} else if closeCell != nil {
		defer func() { _ = closeCell() }()
	}
	svc := service.NewDialogueService(uc, sf)

	grpcSrv := server.NewGRPCServer(&cfg, svc)
	httpSrv := server.NewHTTPServer(&cfg)

	// 7. 会话过期清理 goroutine(随进程退出而停)
	sweepCtx, cancelSweep := context.WithCancel(context.Background())
	defer cancelSweep()
	go runSessionSweep(sweepCtx, sessions, helper)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"tree_count", len(trees),
		"session_ttl", cfg.Dialogue.SessionTTL.Std().String(),
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

// buildTrees 把 yaml 配置的对话树转成内部领域类型,并做基本一致性校验。
func buildTrees(specs []conf.TreeConf) (map[uint32]*data.DialogueTree, error) {
	trees := make(map[uint32]*data.DialogueTree, len(specs))
	for i := range specs {
		spec := &specs[i]
		if spec.NpcID == 0 {
			return nil, fmt.Errorf("tree[%d] npc_id required", i)
		}
		if _, dup := trees[spec.NpcID]; dup {
			return nil, fmt.Errorf("duplicate tree for npc_id %d", spec.NpcID)
		}
		nodes := make(map[string]*data.DialogueNode, len(spec.Nodes))
		for j := range spec.Nodes {
			n := &spec.Nodes[j]
			if n.NodeID == "" {
				return nil, fmt.Errorf("npc %d node[%d] node_id required", spec.NpcID, j)
			}
			if _, dup := nodes[n.NodeID]; dup {
				return nil, fmt.Errorf("npc %d duplicate node_id %q", spec.NpcID, n.NodeID)
			}
			opts := make([]data.DialogueOption, 0, len(n.Options))
			for k := range n.Options {
				o := &n.Options[k]
				if o.OptionID == "" {
					return nil, fmt.Errorf("npc %d node %q option[%d] option_id required", spec.NpcID, n.NodeID, k)
				}
				opts = append(opts, data.DialogueOption{
					OptionID: o.OptionID,
					Text:     o.Text,
					Visible:  o.Visible == nil || *o.Visible, // 省略 = 可见
					NextNode: o.NextNode,
				})
			}
			nodes[n.NodeID] = &data.DialogueNode{NodeID: n.NodeID, Text: n.Text, Options: opts}
		}
		if spec.StartNode == "" {
			return nil, fmt.Errorf("npc %d start_node required", spec.NpcID)
		}
		if _, ok := nodes[spec.StartNode]; !ok {
			return nil, fmt.Errorf("npc %d start_node %q not found in nodes", spec.NpcID, spec.StartNode)
		}
		trees[spec.NpcID] = &data.DialogueTree{
			NpcID:     spec.NpcID,
			Speaker:   spec.Speaker,
			StartNode: spec.StartNode,
			Nodes:     nodes,
		}
	}
	return trees, nil
}

// runSessionSweep 周期清理过期会话,防止被遗弃的会话堆积。
func runSessionSweep(ctx context.Context, store *data.MemorySessionStore, helper *klog.Helper) {
	ticker := time.NewTicker(sessionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,下轮继续。
			safego.Run(ctx, "dialogue_session_sweep", func() {
				if n := store.SweepExpired(time.Now().UnixMilli()); n > 0 {
					helper.Infow("msg", "dialogue_sessions_swept", "count", n)
				}
			})
		}
	}
}
