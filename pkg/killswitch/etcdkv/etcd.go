// Package etcdkv 提供 Kill-Switch 的 etcd 开关源(opt-in)。
//
// 为什么单独成一个 module:etcd client(go.etcd.io/etcd/client/v3)依赖较重,
// 不想让核心 pkg/killswitch 及所有业务服务无条件背上这个依赖。
// 做成独立 module + driver 注册(init 里 RegisterSource("etcd", ...)),
// 只有真正用 etcd 的服务在 main 里 blank import 才会拉这个依赖:
//
//	import (
//	    _ "github.com/luyuancpp/pandora/pkg/killswitch/etcdkv" // 启用 etcd 源
//	)
//
// 然后配置 killswitch.source=etcd + etcd_endpoints,BaseContext 自动用上。
//
// etcd 数据模型:prefix(默认 /pandora/killswitch/)下每个 key 是一条规则,
// key 去掉 prefix = 规则模式,value = 关停原因(可空)。例如:
//
//	etcdctl put /pandora/killswitch/pandora.match.v1.MatchService/StartMatch "fixing bug #123"
//	etcdctl put /pandora/killswitch/pandora.match.v1.MatchService/*          "match maintenance"
//	etcdctl put "/pandora/killswitch/feature/trade"                          "trade frozen"
//	etcdctl put "/pandora/killswitch/*"                                      "full maintenance"
//	etcdctl del /pandora/killswitch/pandora.match.v1.MatchService/StartMatch  # 恢复
package etcdkv

import (
	"context"
	"strings"
	"sync"
	"time"

	klog "github.com/go-kratos/kratos/v2/log"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/luyuancpp/pandora/pkg/killswitch"
)

const defaultPrefix = "/pandora/killswitch/"

func init() {
	killswitch.RegisterSource("etcd", newEtcdSource)
}

type etcdSource struct {
	cli    *clientv3.Client
	prefix string
	mgr    *killswitch.Manager

	mu    sync.Mutex
	rules map[string]string // 本地可变副本,watch 增量维护后整体 Replace

	cancel    context.CancelFunc
	closeOnce sync.Once
}

func newEtcdSource(cfg killswitch.Config) (killswitch.Source, error) {
	prefix := cfg.EtcdPrefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	dial := cfg.EtcdDialTimeout
	if dial <= 0 {
		dial = 5 * time.Second
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: dial,
	})
	if err != nil {
		return nil, err
	}

	es := &etcdSource{
		cli:    cli,
		prefix: prefix,
		mgr:    killswitch.NewManager(),
		rules:  map[string]string{},
	}

	// 首次全量加载。
	if err := es.loadAll(dial); err != nil {
		_ = cli.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	es.cancel = cancel
	go es.watchLoop(ctx)

	return es, nil
}

func (es *etcdSource) Manager() *killswitch.Manager { return es.mgr }

func (es *etcdSource) Close() error {
	es.closeOnce.Do(func() {
		if es.cancel != nil {
			es.cancel()
		}
		if es.cli != nil {
			_ = es.cli.Close()
		}
	})
	return nil
}

// loadAll 拉 prefix 下全量 key,重建快照。
func (es *etcdSource) loadAll(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := es.cli.Get(ctx, es.prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	es.mu.Lock()
	es.rules = make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if pattern := es.patternOf(string(kv.Key)); pattern != "" {
			es.rules[pattern] = string(kv.Value)
		}
	}
	snapshot := es.copyRules()
	es.mu.Unlock()

	es.mgr.Replace(snapshot)
	klog.Infof("[killswitch] etcd loaded prefix=%s rules=%d", es.prefix, len(snapshot))
	return nil
}

// watchLoop 监听 prefix 增量事件,维护本地副本并 Replace。
// watch 通道意外关闭(网络抖动 / 重连)时,重做一次全量加载再继续。
func (es *etcdSource) watchLoop(ctx context.Context) {
	for {
		wch := es.cli.Watch(ctx, es.prefix, clientv3.WithPrefix())
		for wresp := range wch {
			if err := wresp.Err(); err != nil {
				klog.Warnf("[killswitch] etcd watch error: %v", err)
				break
			}
			es.mu.Lock()
			for _, ev := range wresp.Events {
				pattern := es.patternOf(string(ev.Kv.Key))
				if pattern == "" {
					continue
				}
				switch ev.Type {
				case clientv3.EventTypePut:
					es.rules[pattern] = string(ev.Kv.Value)
					// killswitch 真正"生效"的时刻:某 RPC 被关停。此前只在全量 reload 打 rules=N 计数,
					// "某 RPC 何时被关停/恢复"这一关键运维事件不可观测。切换频率极低,打全上下文零噪音。
					klog.Warnf("[killswitch] rule APPLIED (kill) op=%s reason=%q", pattern, string(ev.Kv.Value))
				case clientv3.EventTypeDelete:
					delete(es.rules, pattern)
					klog.Infof("[killswitch] rule CLEARED (restore) op=%s", pattern)
				}
			}
			snapshot := es.copyRules()
			es.mu.Unlock()
			es.mgr.Replace(snapshot)
		}
		// 通道关闭:ctx 取消则退出,否则重连 + 全量补偿。
		select {
		case <-ctx.Done():
			return
		default:
			klog.Warnf("[killswitch] etcd watch channel closed, reloading and re-watching")
			if err := es.loadAll(5 * time.Second); err != nil {
				klog.Warnf("[killswitch] etcd reload after watch close failed: %v", err)
				time.Sleep(time.Second)
			}
		}
	}
}

// patternOf 把 etcd key 去掉 prefix 得到规则模式;不在 prefix 下返回 ""。
func (es *etcdSource) patternOf(key string) string {
	if !strings.HasPrefix(key, es.prefix) {
		return ""
	}
	return strings.TrimPrefix(key, es.prefix)
}

func (es *etcdSource) copyRules() map[string]string {
	out := make(map[string]string, len(es.rules))
	for k, v := range es.rules {
		out[k] = v
	}
	return out
}
