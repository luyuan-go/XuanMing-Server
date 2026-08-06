// Package conf 是 data_service 服务的私有配置结构(2026-06-16)。
package conf

import (
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
)

// Config 是 data_service 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Data DataConf `yaml:"data" json:"data"`
}

// DataConf 是 data_service 服务私有配置。
type DataConf struct {
	// CacheTTL Redis 缓存条目存活时长(默认 5m)。
	// cache-aside:读 miss 回填时按此 TTL,写后删缓存。
	CacheTTL config.Duration `yaml:"cache_ttl,omitempty" json:"cache_ttl,omitempty"`
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发非预期行为。
func (c *Config) Defaults() {
	if c.Data.CacheTTL <= 0 {
		c.Data.CacheTTL = config.Duration(5 * time.Minute)
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20003"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21003"
	}
}
