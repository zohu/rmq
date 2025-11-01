package rmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zohu/zid"
	"github.com/zohu/zlog"
)

type Topic Prefix

func (t Topic) String() string {
	return Prefix(t).String()
}

type EventType int

func (t EventType) String() string {
	return strconv.Itoa(int(t))
}

//goland:noinspection GoCommentStart
const (
	// 发送消息时
	NewMessageEvent EventType = iota + 1
	// 消息到达传递时间时发出
	ReadyEvent
	// 消息已发送给消费者时
	DeliveredEvent
	// 消费成功
	AckEvent
	// 消费失败
	NackEvent
	// 重新发送消息
	RetryEvent
	// 已达最大重试次数
	FinalFailedEvent
)

const (
	StatePending    = "pending"
	StateReady      = "ready"
	StateReadyRetry = "ready_to_retry"
	StateConsuming  = "consuming"
	StateUnknown    = "unknown"
)

type Event struct {
	Code      EventType
	Timestamp int64
	MsgCount  int
}

func encodeEvent(e *Event) string {
	return e.Code.String() +
		" " + strconv.FormatInt(e.Timestamp, 10) +
		" " + strconv.Itoa(e.MsgCount)
}
func decodeEvent(payload string) (*Event, error) {
	items := strings.Split(payload, " ")
	if len(items) != 3 {
		return nil, errors.New("[decode event error! wrong item count, payload: " + payload)
	}
	code, err := strconv.Atoi(items[0])
	if err != nil {
		return nil, errors.New("decode event error! wrong event code, payload: " + payload)
	}
	timestamp, err := strconv.ParseInt(items[1], 10, 64)
	if err != nil {
		return nil, errors.New("decode event error! wrong timestamp, payload: " + payload)
	}
	count, err := strconv.Atoi(items[2])
	if err != nil {
		return nil, errors.New("decode event error! wrong msg count, payload: " + payload)
	}
	return &Event{
		Code:      EventType(code),
		Timestamp: timestamp,
		MsgCount:  count,
	}, nil
}

type DelayQueue struct {
	topic              Topic
	prefix             string
	pendingKey         string // sort set: 消息id -> 传递时间
	readyKey           string // list
	unAckKey           string // sort set: 消息id -> 重试时间
	retryKey           string // list
	retryCountKey      string // hash: 哈希: 消息id -> 剩余重试计数
	garbageKey         string // set: 消息id
	reportKey          string
	useHashTag         bool
	ticker             *time.Ticker
	close              chan struct{}
	maxConsumeDuration time.Duration // default 5 seconds
	msgTTL             time.Duration // default 24 hour
	retryCount         uint          // default 3
	fetchInterval      time.Duration // default 1 second
	fetchLimit         uint          // default no limit
	fetchCount         int32         // actually running task number
	concurrent         uint          // default 1, executed serially
	sha1map            map[string]string
	sha1mapMu          *sync.RWMutex
	scriptPreload      bool
	consumeBuffer      chan string

	eventListener       EventListener
	nackRedeliveryDelay time.Duration
}
type EventListener interface {
	OnEvent(*Event)
}

var queueOpts = make(map[Topic][]QueueOption)

func NewQueue(topic Topic, opts ...QueueOption) *DelayQueue {
	if Rds() == nil {
		zlog.Panic("rmq: redis not initialized")
	}
	option := &queueOptions{}
	for _, opt := range opts {
		opt(option)
	}
	if err := option.validate(); err != nil {
		zlog.Panic("rmq: queue options error", err)
	}
	prefix := "queue:" + topic.String()
	if option.prefix != "" {
		prefix = option.prefix.String(prefix)
	}
	if *option.hashTag == true {
		prefix = "{" + prefix + "}"
	}
	queueOpts[topic] = opts
	return &DelayQueue{
		topic:               topic,
		prefix:              prefix,
		pendingKey:          prefix + ":pending",
		readyKey:            prefix + ":ready",
		unAckKey:            prefix + ":unack",
		retryKey:            prefix + ":retry",
		retryCountKey:       prefix + ":retry:cnt",
		garbageKey:          prefix + ":garbage",
		reportKey:           prefix + ":report",
		useHashTag:          *option.hashTag,
		fetchLimit:          option.fetchLimit,
		fetchInterval:       option.fetchInterval,
		maxConsumeDuration:  option.maxConsumeDuration,
		concurrent:          option.concurrent,
		retryCount:          option.retryCount,
		nackRedeliveryDelay: option.nackRedeliveryDelay,
		msgTTL:              option.ttl,
		sha1map:             make(map[string]string),
		sha1mapMu:           &sync.RWMutex{},
	}
}
func (q *DelayQueue) SendScheduleMsg(payload string, t time.Time) (string, error) {
	ctx := context.Background()
	rds := Rds()
	id := zid.NextString()
	msgTTL := t.Sub(time.Now()) + q.msgTTL
	err := rds.Set(ctx, q.genMsgKey(id), payload, msgTTL).Err()
	if err != nil {
		return "", fmt.Errorf("set failed: %w", err)
	}
	err = rds.HSet(ctx, q.retryCountKey, id, strconv.Itoa(int(q.retryCount))).Err()
	if err != nil {
		return "", fmt.Errorf("hset failed: %w", err)
	}
	err = rds.ZAdd(ctx, q.pendingKey, rds.FloatToZ(map[string]float64{id: float64(t.Unix())})...).Err()
	if err != nil {
		return "", fmt.Errorf("zadd failed: %w", err)
	}
	q.reportEvent(NewMessageEvent, 1)
	return id, nil
}
func (q *DelayQueue) SendDelayMsg(payload string, duration time.Duration) (string, error) {
	return q.SendScheduleMsg(payload, time.Now().Add(duration))
}

type InterceptResult struct {
	Intercepted bool
	State       string
}

func (q *DelayQueue) TryIntercept(id string) (*InterceptResult, error) {
	ctx := context.Background()
	rds := Rds()
	removed, err := rds.LRem(ctx, q.readyKey, 0, id).Result()
	if err != nil {
		zlog.Warnf("intercept %s from ready failed: %v", id, err)
	}
	if removed > 0 {
		_ = rds.Del(ctx, q.genMsgKey(id))
		_ = rds.HDel(ctx, q.retryCountKey, id)
		return &InterceptResult{
			Intercepted: true,
			State:       StateReady,
		}, nil
	}
	removed, err = rds.ZRem(ctx, q.pendingKey, []string{id}).Result()
	if err != nil {
		zlog.Warnf("intercept %s from pending failed: %v", id, err)
	}
	if removed > 0 {
		_ = rds.Del(ctx, q.genMsgKey(id))
		_ = rds.HDel(ctx, q.retryCountKey, id)
		return &InterceptResult{
			Intercepted: true,
			State:       StatePending,
		}, nil
	}
	rds.HDel(ctx, q.retryCountKey, id)
	rds.LRem(ctx, q.retryKey, 0, id)
	return &InterceptResult{
		Intercepted: false,
		State:       StateUnknown,
	}, nil
}
func (q *DelayQueue) Subscribe(cb func(payload string) bool) (done <-chan struct{}) {
	q.close = make(chan struct{}, 1)
	q.ticker = time.NewTicker(q.fetchInterval)
	q.consumeBuffer = make(chan string, q.fetchLimit)
	dc := make(chan struct{})
	for i := 0; i < int(q.concurrent); i++ {
		q.goWithRecover(func() {
			for id := range q.consumeBuffer {
				_ = q.callback(id, cb)
			}
		})
	}
	go func() {
	tickerLoop:
		for {
			select {
			case <-q.ticker.C:
				ids, err := q.beforeConsume()
				if err != nil {
					zlog.Warnf("before consume error: %v", err)
				}
				q.goWithRecover(func() {
					for _, id := range ids {
						q.consumeBuffer <- id
					}
				})
				err = q.afterConsume()
				if err != nil {
					zlog.Warnf("after consume error: %v", err)
				}
			case <-q.close:
				break tickerLoop
			}
		}
		close(dc)
	}()
	return dc
}
func (q *DelayQueue) Stop() {
	close(q.close)
	if q.ticker != nil {
		q.ticker.Stop()
	}
}
func (q *DelayQueue) GetPendingCount() (int64, error) {
	ctx := context.Background()
	rds := Rds()
	return rds.ZCard(ctx, q.pendingKey).Result()
}
func (q *DelayQueue) GetReadyCount() (int64, error) {
	ctx := context.Background()
	rds := Rds()
	return rds.LLen(ctx, q.readyKey).Result()
}
func (q *DelayQueue) GetProcessingCount() (int64, error) {
	ctx := context.Background()
	rds := Rds()
	return rds.ZCard(ctx, q.unAckKey).Result()
}
func (q *DelayQueue) ListenEvent(listener EventListener) {
	q.eventListener = listener
}
func (q *DelayQueue) DisableListener() {
	q.eventListener = nil
}
func (q *DelayQueue) EnableReport() {
	q.ListenEvent(&pubSubListener{reportChan: q.reportKey})
}
func (q *DelayQueue) DisableReport() {
	q.DisableListener()
}

func (q *DelayQueue) loadScript(script string) (string, error) {
	ctx := context.Background()
	rds := Rds()
	sha1, err := rds.ScriptLoad(ctx, script).Result()
	if err != nil {
		return "", err
	}
	q.sha1mapMu.Lock()
	q.sha1map[script] = sha1
	q.sha1mapMu.Unlock()
	return sha1, nil
}
func (q *DelayQueue) eval(script string, keys []string, args []interface{}) (interface{}, error) {
	ctx := context.Background()
	rds := Rds()
	var err error
	q.sha1mapMu.RLock()
	sha1, ok := q.sha1map[script]
	q.sha1mapMu.RUnlock()
	if !ok {
		sha1, err = q.loadScript(script)
		if err != nil {
			return nil, err
		}
	}
	result, err := rds.EvalSha(ctx, sha1, keys, args).Result()
	if err == nil {
		return result, err
	}
	if strings.HasPrefix(err.Error(), "NOSCRIPT") {
		sha1, err = q.loadScript(script)
		if err != nil {
			return nil, err
		}
		result, err = rds.EvalSha(ctx, sha1, keys, args).Result()
	}
	return result, err
}

const pending2ReadyScript = `
local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get ready msg
if (#msgs == 0) then return end
local args2 = {} -- keys to push into ready
for _,v in ipairs(msgs) do
	table.insert(args2, v) 
    if (#args2 == 4000) then
		redis.call('LPush', KEYS[2], unpack(args2))
		args2 = {}
	end
end
if (#args2 > 0) then 
	redis.call('LPush', KEYS[2], unpack(args2))
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from pending
return #msgs
`

func (q *DelayQueue) pending2Ready() error {
	now := time.Now().Unix()
	keys := []string{q.pendingKey, q.readyKey}
	raw, err := q.eval(pending2ReadyScript, keys, []interface{}{now})
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("pending2ReadyScript failed: %v", err)
	}
	count, ok := raw.(int64)
	if ok {
		q.reportEvent(ReadyEvent, int(count))
	}
	return nil
}

const ready2UnackScript = `
local msg = redis.call('RPop', KEYS[1])
if (not msg) then return end
redis.call('ZAdd', KEYS[2], ARGV[1], msg)
return msg
`

func (q *DelayQueue) ready2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	keys := []string{q.readyKey, q.unAckKey}
	ret, err := q.eval(ready2UnackScript, keys, []interface{}{retryTime})
	if errors.Is(err, redis.Nil) {
		return "", err
	}
	if err != nil {
		return "", fmt.Errorf("ready2UnackScript failed: %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	q.reportEvent(DeliveredEvent, 1)
	return str, nil
}

const retry2UnackScript = `
local msg = redis.call('RPop', KEYS[1])
if (not msg) then return end
redis.call('ZAdd', KEYS[2], ARGV[1], msg)
return msg
`

func (q *DelayQueue) retry2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	keys := []string{q.retryKey, q.unAckKey}
	ret, err := q.eval(retry2UnackScript, keys, []interface{}{retryTime})
	if errors.Is(err, redis.Nil) {
		return "", redis.Nil
	}
	if err != nil {
		return "", fmt.Errorf("retry2UnackScript failed: %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	q.reportEvent(DeliveredEvent, 1)
	return str, nil
}
func (q *DelayQueue) callback(id string, cb func(payload string) bool) error {
	ctx := context.Background()
	rds := Rds()
	payload, err := rds.Get(ctx, q.genMsgKey(id)).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get message payload failed: %v", err)
	}
	ack := cb(payload)
	if ack {
		err = q.ack(id)
	} else {
		err = q.nack(id)
	}
	return err
}
func (q *DelayQueue) ack(id string) error {
	ctx := context.Background()
	rds := Rds()
	atomic.AddInt32(&q.fetchCount, -1)
	_, err := rds.ZRem(ctx, q.unAckKey, id).Result()
	if err != nil {
		return fmt.Errorf("remove from unack failed: %v", err)
	}
	_ = rds.Del(ctx, q.genMsgKey(id))
	_ = rds.HDel(ctx, q.retryCountKey, id)
	q.reportEvent(AckEvent, 1)
	return nil
}

const updateZSetScoreScript = `
if redis.call('zscore', KEYS[1], ARGV[2]) then
    redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
    return 1
else
    return -1
end
`

func (q *DelayQueue) updateZSetScore(key string, score float64, member string) error {
	scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
	res, err := q.eval(updateZSetScoreScript, []string{key}, []interface{}{scoreStr, member})
	if err != nil {
		return err
	}
	if res == int64(-1) {
		return errors.New("not found")
	}
	return nil
}

func (q *DelayQueue) nack(id string) error {
	atomic.AddInt32(&q.fetchCount, -1)

	ctx := context.Background()
	rds := Rds()

	// 检查是否还有重试次数（只读，不修改）
	retryStr, err := rds.HGet(ctx, q.retryCountKey, id).Result()
	if errors.Is(err, redis.Nil) {
		// 没有记录，丢弃
		_ = rds.ZRem(ctx, q.unAckKey, id)
		_ = rds.Del(ctx, q.genMsgKey(id))
		q.reportEvent(FinalFailedEvent, 1)
		return nil
	}
	if err != nil {
		return fmt.Errorf("get retry count failed: %w", err)
	}

	retryCount, _ := strconv.Atoi(retryStr)
	if retryCount <= 0 {
		_ = rds.ZRem(ctx, q.unAckKey, id)
		_ = rds.Del(ctx, q.genMsgKey(id))
		_ = rds.HDel(ctx, q.retryCountKey, id)
		q.reportEvent(FinalFailedEvent, 1)
		return nil
	}

	// 关键： 只更新 unack 的 score
	retryTime := time.Now().Add(q.nackRedeliveryDelay).Unix()
	err = q.updateZSetScore(q.unAckKey, float64(retryTime), id)
	if err != nil {
		return fmt.Errorf("update unack score failed: %w", err)
	}

	q.reportEvent(NackEvent, 1)
	return nil
}

const unack2RetryScript = `
local unack2retry = function(msgs)
	local retryCounts = redis.call('HMGet', KEYS[2], unpack(msgs)) -- get retry count
	local retryMsgs = 0
	local failMsgs = 0
	for i,v in ipairs(retryCounts) do
		local k = msgs[i]
		if v ~= false and v ~= nil and v ~= '' and tonumber(v) > 0 then
			redis.call("HIncrBy", KEYS[2], k, -1) -- reduce retry count
			redis.call("LPush", KEYS[3], k) -- add to retry
			retryMsgs = retryMsgs + 1
		else
			redis.call("HDel", KEYS[2], k) -- del retry count
			redis.call("SAdd", KEYS[4], k) -- add to garbage
			failMsgs = failMsgs + 1
		end
	end
	return retryMsgs, failMsgs
end

local retryMsgs = 0
local failMsgs = 0
local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get retry msg
if (#msgs == 0) then return end
if #msgs < 4000 then
	local d1, d2 = unack2retry(msgs)
	retryMsgs = retryMsgs + d1
	failMsgs = failMsgs + d2
else
	local buf = {}
	for _,v in ipairs(msgs) do
		table.insert(buf, v)
		if #buf == 4000 then
		    local d1, d2 = unack2retry(buf)
			retryMsgs = retryMsgs + d1
			failMsgs = failMsgs + d2
			buf = {}
		end
	end
	if (#buf > 0) then
		local d1, d2 = unack2retry(buf)
		retryMsgs = retryMsgs + d1
		failMsgs = failMsgs + d2
	end
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from unack
return {retryMsgs, failMsgs}
`

func (q *DelayQueue) unack2Retry() error {
	keys := []string{q.unAckKey, q.retryCountKey, q.retryKey, q.garbageKey}
	now := time.Now()
	raw, err := q.eval(unack2RetryScript, keys, []interface{}{now.Unix()})
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("unack to retry script failed: %v", err)
	}
	infos, ok := raw.([]interface{})
	if ok && len(infos) == 2 {
		retryCount, ok := infos[0].(int64)
		if ok {
			q.reportEvent(RetryEvent, int(retryCount))
		}
		failCount, ok := infos[1].(int64)
		if ok {
			q.reportEvent(FinalFailedEvent, int(failCount))
		}
	}
	return nil
}
func (q *DelayQueue) garbageCollect() error {
	ctx := context.Background()
	rds := Rds()
	msgIds, err := rds.SMembers(ctx, q.garbageKey).Result()
	if err != nil {
		return fmt.Errorf("smembers failed: %v", err)
	}
	if len(msgIds) == 0 {
		return nil
	}
	msgKeys := make([]string, 0, len(msgIds))
	for _, idStr := range msgIds {
		msgKeys = append(msgKeys, q.genMsgKey(idStr))
	}
	err = rds.Del(ctx, msgKeys...).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("del msgs failed: %v", err)
	}
	err = rds.SRem(ctx, q.garbageKey, msgIds).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("remove from garbage key failed: %v", err)
	}
	return nil
}
func (q *DelayQueue) reportEvent(code EventType, count int) {
	listener := q.eventListener
	if listener != nil && count > 0 {
		event := &Event{
			Code:      code,
			Timestamp: time.Now().Unix(),
			MsgCount:  count,
		}
		listener.OnEvent(event)
	}
}
func (q *DelayQueue) beforeConsume() ([]string, error) {
	// pending to ready
	err := q.pending2Ready()
	if err != nil {
		return nil, err
	}
	// ready2Unack
	// prioritize new message consumption to avoid avalanches
	ids := make([]string, 0, q.fetchLimit)
	var fetchCount int32
	for {
		fetchCount = atomic.LoadInt32(&q.fetchCount)
		if q.fetchLimit > 0 && fetchCount >= int32(q.fetchLimit) {
			break
		}
		idStr, err := q.ready2Unack()
		if errors.Is(err, redis.Nil) {
			break
		}
		if err != nil {
			return nil, err
		}
		ids = append(ids, idStr)
		atomic.AddInt32(&q.fetchCount, 1)
	}
	if fetchCount < int32(q.fetchLimit) || q.fetchLimit == 0 {
		for {
			fetchCount = atomic.LoadInt32(&q.fetchCount)
			if q.fetchLimit > 0 && fetchCount >= int32(q.fetchLimit) {
				break
			}
			idStr, err := q.retry2Unack()
			if errors.Is(err, redis.Nil) {
				break
			}
			if err != nil {
				return nil, err
			}
			ids = append(ids, idStr)
			atomic.AddInt32(&q.fetchCount, 1)
		}
	}
	return ids, nil
}
func (q *DelayQueue) afterConsume() error {
	err := q.unack2Retry()
	if err != nil {
		return err
	}
	err = q.garbageCollect()
	if err != nil {
		return err
	}
	return nil
}
func (q *DelayQueue) goWithRecover(fn func()) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				zlog.Error("rmq: panic", "err", err)
			}
		}()
		fn()
	}()
}
func (q *DelayQueue) genMsgKey(id string) string {
	return q.prefix + ":msg:" + id
}

type pubSubListener struct {
	reportChan string
}

func (l *pubSubListener) OnEvent(event *Event) {
	ctx := context.Background()
	rds := Rds()
	payload := encodeEvent(event)
	rds.Publish(ctx, l.reportChan, payload)
}
