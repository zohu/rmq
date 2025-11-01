### RMQ缓存库使用文档
#### 介绍
> RMQ(redis memory queue)是一个包含内存和Redis的缓存解决方案，它提供了高级功能如分布式延迟队列、事件监听以及消息重试机制。

#### 核心组件概览
- **Memory**: 基于bigcache实现的内存缓存，增加了ttl能力。
- **Redis**: Redis客户端封装，支持批量删除等额外功能。
- **Cache**: 提供了L1（内存）和L2（Redis）两级缓存。
- **Dict**: 基于Cache实现的字典功能，扩展了回源能力。
- **DelayQueue**: 提供基于Redis+lua实现的延迟队列。
- **Publisher & Monitor**: 分别用于发送消息和监控队列状态。

#### 安装
```shell
go get -u github.com/zohu/rmq
```

#### 使用方法
1. **适用内存缓存**
```go
// 实例化方法
// mem := rmq.NewMemory(opts ...MemoryOption) *Memory

// 如果仅使用全局单例，可以先全局初始化，然后直接使用
rmq.InitMemory(
    WithMemoryPrefix("rmq"),  // 全局前缀，对业务透明, 默认为rmq
)
rmq.Memory().Set("test", "hello world")
rmq.Memory().Get("test")
rmq.Memory().Delete("test")
/* 更多方法参考memory.go */

```
2. **使用Redis缓存**
```go
// 实例化
// rds := rmq.NewRedis(opts ...RedisOption) *Redis

// 如果仅使用全局单例，可以先全局初始化，然后直接使用
rmq.InitRedis(
    WithRedisPrefix("rmq"), // 全局前缀，对业务透明, 默认为rmq
    WithRedisOptions(&redis.UniversalOptions{
        Addrs:      []string{"localhost:8011"},
        Password:   "123456",
        ClientName: "rmq",
    }),
)

rmq.Rds().Set(ctx, "key", "value")
/* 更多方法参考go-redis */
```
3. **使用二级缓存**
```go
// 实例化
// rmq.NewCache(m *Memory, r *Redis) *Cache

// 如果仅使用全局单例，可以先全局初始化，然后直接使用
rmq.InitCache(rmq.Mem(), rmq.Rds())

rmq.Cache().Set(ctx, "key", "value", time.Minute)
rmq.Cache().Get(ctx, "key")
rmq.Cache().Del(ctx, "key")
```

4. **使用字典**
```go
// 使用字典之前，需要先初始化全局Cache
rmq.InitCache(rmq.Mem(), rmq.Rds())

// name=字典名，比如"store_name"
// query=查询函数，返回字典值 func(ctx context.Context, key string) string
// ttl=回源结果的默认有效期
rmq.NewDict(name DictName, query DictQuery, ttl ...time.Duration)

// 使用方法
rmq.Dict(ctx, name DictName, key string) (string, error)
```
5. **使用延迟队列**
```go
// 定义一个主题
const (
	orderTopic rmq.Topic = "order"
)
dq := rmq.NewQueue(orderTopic, opts ...QueueOption) *DelayQueue
// 注册消费者
done := dq.Subscribe(cb func(payload string) bool) (done <-chan struct{})

// 发送消息
dq.SendScheduleMsg(payload string, t time.Time) (string, error)
dq.SendDelayMsg(payload string, duration time.Duration) (string, error)
// 拦截消息
dq.TryIntercept(id string) (*InterceptResult, error)

// 其他方法
dq.Stop()
dq.GetPendingCount() (int64, error)
dq.GetReadyCount() (int64, error)
dq.GetProcessingCount() (int64, error)
dq.ListenEvent(listener EventListener)
dq.DisableListener()
dq.EnableReport()
dq.DisableReport()
```
```go
// 消费者和生产者不在一起
// 同服务内（内部有缓存topic的配置，可以无需再传）：
dq := rmq.NewPublisher(topic) *Publisher

// 跨服务的（必须传入和消费者相同的配置，特别是prefix）
dq := rmq.NewPublisher(topic Topic, opts ...QueueOption) *Publisher

// 监控同理
mnt := rmq.NewMonitor(topic Topic, opts ...QueueOption) *Monitor
```

#### 依赖库
- [github.com/allegro/bigcache/v3](https://github.com/allegro/bigcache)
- [github.com/bytedance/sonic](https://github.com/bytedance/sonic)
- [github.com/redis/go-redis/v9](https://github.com/redis/go-redis)