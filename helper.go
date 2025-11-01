package rmq

import "github.com/zohu/zlog"

var (
	mem   *Memory
	rds   *Redis
	cache *Cache
)

func InitMemory(opts ...MemoryOption) {
	mem = NewMemory(opts...)
}
func InitRedis(opts ...RedisOption) {
	rds = NewRedis(opts...)
}
func InitCache(m *Memory, r *Redis) {
	cache = NewCache(m, r)
}

func Mem() *Memory {
	if mem == nil {
		zlog.Panic("rmq: memory not initialized")
	}
	return mem
}

func Rds() *Redis {
	if rds == nil {
		zlog.Panic("rmq: redis not initialized")
	}
	return rds
}

func CacheL2() *Cache {
	if cache == nil {
		zlog.Panic("rmq: cache not initialized")
	}
	return cache
}
