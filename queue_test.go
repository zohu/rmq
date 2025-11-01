package rmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	InitRedis(WithRedisOptions(&redis.UniversalOptions{
		Addrs:      []string{"localhost:8011"},
		Password:   "JCFkQYex4f",
		ClientName: "rmq",
	}))
}

func TestDelayQueue_FullLifecycle(t *testing.T) {
	if Rds() == nil {
		t.Skip("Redis not initialized")
	}

	t.Parallel()

	queueName := Topic("test_queue_" + time.Now().Format("20060102150405"))
	q := NewQueue(queueName,
		WithQueuePrefix("test"),
		WithQueueConcurrent(1),
		WithQueueFetchLimit(10),
		WithQueueFetchInterval(1*time.Second),
		WithQueueMaxConsumeDuration(5*time.Second),
		WithQueueRetryCount(2),
		WithQueueNackRedeliveryDelay(10*time.Second),
		WithQueueDefaultTTL(60*time.Second),
	)

	var mu sync.Mutex
	consumed := make([]string, 0)
	acked := make([]string, 0)
	nacked := make([]string, 0)

	_, err := q.SendDelayMsg("msg1", 10*time.Millisecond)
	require.NoError(t, err)
	_, err = q.SendDelayMsg("msg2", 10*time.Millisecond)
	require.NoError(t, err)
	_, err = q.SendDelayMsg("msg3", 10*time.Millisecond)
	require.NoError(t, err)
	_, err = q.SendDelayMsg("msg4", 10*time.Millisecond)
	require.NoError(t, err)

	done := q.Subscribe(func(payload string) bool {
		mu.Lock()
		defer mu.Unlock()
		consumed = append(consumed, payload)
		if payload == "msg3" || payload == "msg4" {
			nacked = append(nacked, payload)
			return false
		}
		acked = append(acked, payload)
		return true
	})
	defer func() {
		q.Stop()
		<-done
	}()

	// Phase 1: Only initial messages consumed
	time.Sleep(2 * time.Second)

	mu.Lock()
	assert.ElementsMatch(t, []string{"msg1", "msg2", "msg3", "msg4"}, consumed)
	assert.ElementsMatch(t, []string{"msg1", "msg2"}, acked)
	assert.ElementsMatch(t, []string{"msg3", "msg4"}, nacked)
	mu.Unlock()

	// Phase 2: First retry
	time.Sleep(12 * time.Second)

	mu.Lock()
	assert.Len(t, consumed, 6) // msg3, msg4 retried once
	mu.Unlock()

	// Phase 3: Second (final) retry
	time.Sleep(12 * time.Second)

	mu.Lock()
	assert.Len(t, consumed, 8) // total: 1+1+3+3 = 8
	mu.Unlock()

	// Final cleanup
	time.Sleep(2 * time.Second)

	ctx := context.Background()
	pending, _ := q.GetPendingCount()
	ready, _ := q.GetReadyCount()
	processing, _ := q.GetProcessingCount()
	assert.Equal(t, int64(0), pending)
	assert.Equal(t, int64(0), ready)
	assert.Equal(t, int64(0), processing)

	garbageCount, _ := Rds().SCard(ctx, q.garbageKey).Result()
	assert.Equal(t, int64(0), garbageCount)
}
func TestDelayQueue_Intercept(t *testing.T) {
	if Rds() == nil {
		t.Skip("Redis not initialized")
	}

	queueName := Topic("test_intercept_" + time.Now().Format("20060102150405"))
	q := NewQueue(queueName)

	// Send to pending
	_, err := q.SendDelayMsg("pending_msg", 10*time.Second)
	require.NoError(t, err)

	// Send to ready (by sending with 0 delay)
	id2, err := q.SendDelayMsg("ready_msg", 0)
	require.NoError(t, err)

	// Force move pending to ready
	err = q.pending2Ready()
	require.NoError(t, err)

	// Intercept ready message
	result, err := q.TryIntercept(id2)
	require.NoError(t, err)
	assert.True(t, result.Intercepted)
	assert.Equal(t, StateReady, result.State)

	// Intercept pending message (should be in pending now)
	id3, err := q.SendDelayMsg("pending_msg2", 10*time.Second)
	require.NoError(t, err)
	result, err = q.TryIntercept(id3)
	require.NoError(t, err)
	assert.True(t, result.Intercepted)
	assert.Equal(t, StatePending, result.State)

	// Try intercept non-existing
	result, err = q.TryIntercept("nonexistent")
	require.NoError(t, err)
	assert.False(t, result.Intercepted)
	assert.Equal(t, StateUnknown, result.State)
}

func TestDelayQueue_AfterConsume_NoPanic(t *testing.T) {
	if Rds() == nil {
		t.Skip("Redis not initialized")
	}

	queueName := Topic("test_after_consume_" + time.Now().Format("20060102150405"))
	q := NewQueue(queueName)

	// Should not panic even with empty queues
	err := q.afterConsume()
	assert.NoError(t, err)
}

func TestDelayQueue_Callback_NilPayload(t *testing.T) {
	if Rds() == nil {
		t.Skip("Redis not initialized")
	}

	queueName := "test_nil_payload_" + time.Now().Format("20060102150405")
	q := NewQueue(Topic(queueName))

	// Simulate message with deleted payload
	id := "test_id"
	err := q.nack(id)
	assert.NoError(t, err) // Should not fail even if payload missing

	err = q.ack(id)
	assert.NoError(t, err)
}

func TestDelayQueue_ScriptPreload(t *testing.T) {
	if Rds() == nil {
		t.Skip("Redis not initialized")
	}

	queueName := "test_script_preload_" + time.Now().Format("20060102150405")
	q := NewQueue(Topic(queueName))

	// Trigger script loading
	_, err := q.eval(pending2ReadyScript, []string{"a", "b"}, []interface{}{time.Now().Unix()})
	assert.True(t, err == nil || errors.Is(err, redis.Nil))

	q.sha1mapMu.RLock()
	sha, ok := q.sha1map[pending2ReadyScript]
	q.sha1mapMu.RUnlock()
	assert.True(t, ok)
	assert.NotEmpty(t, sha)
}

func TestDelayQueue_EventReporting(t *testing.T) {
	if Rds() == nil {
		t.Skip("Redis not initialized")
	}

	queueName := "test_event_" + time.Now().Format("20060102150405")
	q := NewQueue(Topic(queueName))

	events := make([]*Event, 0)
	var mu sync.Mutex
	q.ListenEvent(EventListenerFunc(func(e *Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}))

	// Send and consume one message
	_, err := q.SendDelayMsg("test", 0)
	require.NoError(t, err)

	done := q.Subscribe(func(payload string) bool { return true })
	time.Sleep(200 * time.Millisecond)
	q.Stop()
	<-done

	mu.Lock()
	assert.NotEmpty(t, events)
	mu.Unlock()
}

type EventListenerFunc func(*Event)

func (f EventListenerFunc) OnEvent(e *Event) {
	f(e)
}

func TestDelayQueue_DecodeEvent(t *testing.T) {
	payload := "1 1710000000 5"
	event, err := decodeEvent(payload)
	require.NoError(t, err)
	assert.Equal(t, NewMessageEvent, event.Code)
	assert.Equal(t, int64(1710000000), event.Timestamp)
	assert.Equal(t, 5, event.MsgCount)

	_, err = decodeEvent("invalid")
	assert.Error(t, err)

	_, err = decodeEvent("1 1710000000")
	assert.Error(t, err)
}

func TestDelayQueue_UpdateZSetScore(t *testing.T) {
	if Rds() == nil {
		t.Skip("Redis not initialized")
	}

	queueName := "test_zset_score_" + time.Now().Format("20060102150405")
	q := NewQueue(Topic(queueName))

	ctx := context.Background()
	rds := Rds()
	key := q.prefix + ":test_zset"
	member := "test_member"

	// Add initial score
	err := rds.ZAdd(ctx, key, redis.Z{Score: 100, Member: member}).Err()
	require.NoError(t, err)

	// Update score
	err = q.updateZSetScore(key, 200, member)
	assert.NoError(t, err)

	score, err := rds.ZScore(ctx, key, member).Result()
	require.NoError(t, err)
	assert.Equal(t, float64(200), score)

	// Update non-existing member (should not add)
	err = q.updateZSetScore(key, 300, "nonexistent")
	assert.Error(t, err) // 应该报错

	_, err = rds.ZRank(ctx, key, "nonexistent").Result()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, redis.Nil))
}

func TestDelayQueue_GarbageCollect(t *testing.T) {
	if Rds() == nil {
		t.Skip("Redis not initialized")
	}

	queueName := "test_gc_" + time.Now().Format("20060102150405")
	q := NewQueue(Topic(queueName))

	ctx := context.Background()
	rds := Rds()

	// Add garbage
	id := "gc_test_id"
	err := rds.SAdd(ctx, q.garbageKey, id).Err()
	require.NoError(t, err)
	err = rds.Set(ctx, q.genMsgKey(id), "payload", 10*time.Second).Err()
	require.NoError(t, err)

	// Run GC
	err = q.garbageCollect()
	assert.NoError(t, err)

	// Verify cleaned
	exists, _ := rds.Exists(ctx, q.genMsgKey(id)).Result()
	assert.Equal(t, int64(0), exists)

	garbageExists, _ := rds.SIsMember(ctx, q.garbageKey, id).Result()
	assert.False(t, garbageExists)
}
