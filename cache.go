package rmq

import (
	"context"
	"time"

	"github.com/zohu/zlog"
)

type Cache struct {
	mem *Memory
	rds *Redis
}

func NewCache(m *Memory, r *Redis) *Cache {
	if m.prefix != r.prefix {
		zlog.Panic("rmq: memory and redis prefix not match")
	}
	return &Cache{
		mem: m,
		rds: r,
	}
}

func (c *Cache) Set(ctx context.Context, key string, value string, ttl time.Duration) {
	c.rds.Set(ctx, key, value, ttl)
	c.mem.Set(key, value, c.l1(ttl))
}
func (c *Cache) Get(ctx context.Context, k string) (string, error) {
	if v, ok := c.mem.Get(k); ok {
		return v, nil
	}
	v, err := c.rds.Get(ctx, k).Result()
	if err != nil {
		return "", err
	}
	exp := c.rds.ExpireTime(ctx, k).Val()
	if exp > 0 {
		c.mem.Set(k, v, c.l1(exp))
	}
	return v, nil
}
func (c *Cache) Del(ctx context.Context, k string) error {
	c.mem.Delete(k)
	return c.rds.Del(ctx, k).Err()
}
func (c *Cache) l1(expiration time.Duration) time.Duration {
	if expiration > 10*time.Minute {
		return 10 * time.Minute
	}
	return expiration
}
