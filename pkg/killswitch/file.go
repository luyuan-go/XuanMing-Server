// Package killswitch — file 源
//
// 监听一个 yaml 文件,改文件即热生效(dev / 无 etcd 时用)。
//
// yaml 格式(key = 规则模式,value = 关停原因,可留空):
//
//	rules:
//	  "pandora.login.v1.LoginService/Login": "fixing bug #123"
//	  "pandora.match.v1.MatchService/*": "matchmaker maintenance"
//	  "feature/trade": "trade frozen for hotfix"
//	  # "*": "full maintenance"   # 全局维护,慎用
//
// 留空 / 删光 rules = 全部恢复(放行)。文件不存在按空集处理(不报错)。
package killswitch

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	klog "github.com/go-kratos/kratos/v2/log"
	"gopkg.in/yaml.v3"
)

func init() {
	RegisterSource("file", newFileSource)
}

// fileRules 是 yaml 文件的解析目标。
type fileRules struct {
	Rules map[string]string `yaml:"rules"`
}

// fileSource 用 fsnotify 监听 yaml 文件并维护 Manager。
type fileSource struct {
	path    string
	mgr     *Manager
	watcher *fsnotify.Watcher

	closeOnce sync.Once
	done      chan struct{}
}

func newFileSource(cfg Config) (Source, error) {
	path := cfg.FilePath
	if path == "" {
		path = "etc/killswitch.yaml"
	}
	abs, err := filepath.Abs(path)
	if err == nil {
		path = abs
	}

	fs := &fileSource{
		path: path,
		mgr:  NewManager(),
		done: make(chan struct{}),
	}

	// 首次全量加载(文件缺失不报错,按空集)。
	fs.reload()

	w, err := fsnotify.NewWatcher()
	if err != nil {
		// 监听器建不起来:仍返回已加载一次的源(降级为「静态一次性」),不阻断启动。
		klog.Warnf("[killswitch] file watcher init failed, using static snapshot: %v", err)
		return fs, nil
	}
	fs.watcher = w
	// 监听父目录:编辑器多为「写临时文件 + rename」,直接 watch 文件会丢事件。
	if err := w.Add(filepath.Dir(path)); err != nil {
		klog.Warnf("[killswitch] watch dir failed, using static snapshot: %v", err)
		_ = w.Close()
		fs.watcher = nil
		return fs, nil
	}
	go fs.watchLoop()
	return fs, nil
}

func (fs *fileSource) Manager() *Manager { return fs.mgr }

func (fs *fileSource) Close() error {
	fs.closeOnce.Do(func() {
		close(fs.done)
		if fs.watcher != nil {
			_ = fs.watcher.Close()
		}
	})
	return nil
}

// reload 读文件、解析、替换快照。文件缺失 / 解析失败按「保留旧值」更安全,
// 但缺失我们按空集(放行);解析失败保留旧快照避免误关。
func (fs *fileSource) reload() {
	data, err := os.ReadFile(fs.path)
	if err != nil {
		if os.IsNotExist(err) {
			fs.mgr.Replace(map[string]string{})
			return
		}
		klog.Warnf("[killswitch] read %s failed, keep previous rules: %v", fs.path, err)
		return
	}
	rules, err := parseRules(data)
	if err != nil {
		klog.Warnf("[killswitch] parse %s failed, keep previous rules: %v", fs.path, err)
		return
	}
	fs.mgr.Replace(rules)
	// 只打条数无法回答「当前关停了哪些 op」——排障时运维得去翻文件/etcd。规则集很小,
	// 直接列出生效的 operation 清单(零噪音、纯收益)。
	ops := make([]string, 0, len(rules))
	for pattern := range rules {
		ops = append(ops, pattern)
	}
	sort.Strings(ops)
	klog.Infof("[killswitch] file reloaded %s rules=%d killed_ops=%v", fs.path, len(rules), ops)
}

// parseRules 解析 yaml 成规范化规则 map(key 去前导 "/")。
func parseRules(data []byte) (map[string]string, error) {
	var fr fileRules
	if err := yaml.Unmarshal(data, &fr); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(fr.Rules))
	for k, v := range fr.Rules {
		key := k
		if key != "*" {
			// 规范化:去前导 "/",与 Manager.Disabled 的比对口径一致。
			for len(key) > 0 && key[0] == '/' {
				key = key[1:]
			}
		}
		if key == "" {
			continue
		}
		out[key] = v
	}
	return out, nil
}

func (fs *fileSource) watchLoop() {
	const debounce = 200 * time.Millisecond
	var timer *time.Timer
	target := fs.path
	for {
		select {
		case <-fs.done:
			return
		case ev, ok := <-fs.watcher.Events:
			if !ok {
				return
			}
			// 只关心目标文件(同目录其它文件忽略)。
			if filepath.Clean(ev.Name) != target {
				continue
			}
			// 去抖:编辑器一次保存可能触发多次事件。
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(debounce, fs.reload)
		case err, ok := <-fs.watcher.Errors:
			if !ok {
				return
			}
			klog.Warnf("[killswitch] file watch error: %v", err)
		}
	}
}
