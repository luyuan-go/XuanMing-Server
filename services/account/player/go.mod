module github.com/luyuancpp/pandora/services/account/player

go 1.26.5

// W4 ④ player 服务(Pandora 第 8 个 Kratos 业务服,2026-06-06)。
//
// 职责(docs/design/go-services.md §2.2):玩家档案 / 段位 MMR / 英雄池
//   消费 pandora.player.update → 幂等 UpdateMMR(不变量 §2,idempotency_key=match_id);
//   提供 GetProfile / UpdateNickname / ListHeroes / UnlockHero / GetMMR / UpdateMMR RPC;
//   GetMMR 作为 battle_result 的真实 MMRReader 数据源(W4 ④ 替换 StaticMMRReader)。
//
// 依赖来源:
//   - pkg/        (公共框架,go.work use)
//   - proto/      (player/v1 + common/v1)
//   - Kratos v2.9.2 / IBM/sarama(kafka 消费 player.update)
//   - go-sql-driver/mysql(pandora_player 库,经 pkg/mysqlx 注册驱动)

require (
	github.com/IBM/sarama v1.43.1
	github.com/go-kratos/kratos/v2 v2.9.2
	github.com/luyuancpp/pandora/pkg v0.0.0
	github.com/luyuancpp/pandora/proto v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/lufia/plan9stats v0.0.0-20230326075908-cb1d2100619a // indirect
	github.com/power-devops/perfstat v0.0.0-20221212215047-62379fc7944b // indirect
	github.com/shirou/gopsutil/v3 v3.23.6 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	go.etcd.io/etcd/api/v3 v3.5.16 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.16 // indirect
	go.etcd.io/etcd/client/v3 v3.5.16 // indirect
)

require (
	dario.cat/mergo v1.0.0 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/eapache/go-resiliency v1.6.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-kratos/aegis v0.2.0 // indirect
	github.com/go-playground/form/v4 v4.2.0 // indirect
	github.com/go-sql-driver/mysql v1.8.1
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/luyuancpp/pandora/pkg/cellroute/etcdtable v0.0.0-00010101000000-000000000000
	github.com/luyuancpp/pandora/pkg/dsauthfence v0.0.0-00010101000000-000000000000
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/prometheus/client_golang v1.21.1 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.52.0 // indirect
	golang.org/x/net v0.54.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// 本地 workspace 内的模块通过 replace 指向源码目录。
replace (
	github.com/luyuancpp/pandora/pkg => ../../../pkg
	github.com/luyuancpp/pandora/proto => ../../../proto
)

replace github.com/luyuancpp/pandora/pkg/cellroute/etcdtable => ../../../pkg/cellroute/etcdtable
