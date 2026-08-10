package offlinewatch

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"
)

// 真 kafka 往返:locator 侧生产 PlayerLeftHubEvent → 本包 consumer 解码 → Enqueue。
// 验证的是「事件确实能把玩家排进复查队列」,即离场事件这条加速链真的通了。
//
//	PANDORA_TEST_REDIS_ADDR=127.0.0.1:6380 PANDORA_TEST_KAFKA_BROKERS=127.0.0.1:9093 \
//	  go test ./pkg/offlinewatch/ -run RealKafka
func TestRealKafka_离场事件排进复查队列(t *testing.T) {
	brokers := os.Getenv("PANDORA_TEST_KAFKA_BROKERS")
	addr := os.Getenv("PANDORA_TEST_REDIS_ADDR")
	if brokers == "" || addr == "" {
		t.Skip("未设 PANDORA_TEST_KAFKA_BROKERS / PANDORA_TEST_REDIS_ADDR,跳过真 kafka 往返")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })

	h := &recordingHandler{}
	w, err := New(client, &fakeReader{}, h, Options{
		Namespace: "realkafka-test", Threshold: 180 * time.Second, Interval: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { client.Del(ctx, w.dueKey, w.evidenceKey) })
	client.Del(ctx, w.dueKey, w.evidenceKey)

	kcfg := config.KafkaConfig{Brokers: []string{brokers}, DialTimeout: config.Duration(5 * time.Second), WriteTimeout: config.Duration(5 * time.Second)}
	consumer, err := w.NewConsumer(kcfg, 1)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	consumer.Start()
	t.Cleanup(func() { _ = consumer.Close() })

	producer, err := kafkax.NewKeyOrderedProducer(kcfg, kafkax.TopicPlayerPresence)
	if err != nil {
		t.Fatalf("NewKeyOrderedProducer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close() })

	const pid uint64 = 9911
	leftAt := time.Now().Add(-200 * time.Second).UnixMilli()
	evt := &locatorv1.PlayerLeftHubEvent{PlayerId: pid, LeftAtMs: leftAt, HubPod: "hub-1"}
	if _, err := proto.Marshal(evt); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := producer.Send(ctx, strconv.FormatUint(pid, 10), evt); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// consumer group 首次 rebalance 需要几秒;轮询等待,不用固定 sleep 碰运气。
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if score, err := client.ZScore(ctx, w.dueKey, strconv.FormatUint(pid, 10)).Result(); err == nil {
			want := float64(leftAt + w.opts.Threshold.Milliseconds())
			if score != want {
				t.Fatalf("到期时刻应为 left_at+threshold: got=%v want=%v", score, want)
			}
			return // 通了
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("45s 内离场事件没能把玩家排进复查队列:kafka producer→consumer→Enqueue 这条加速链不通")
}
