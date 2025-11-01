package rmq

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/zohu/zlog"
)

type Redis struct {
	prefix Prefix
	redis.UniversalClient
}

func NewRedis(opts ...RedisOption) *Redis {
	opt := &redisOptions{}
	for _, o := range opts {
		o(opt)
	}
	if err := opt.validate(); err != nil {
		zlog.Panic("rmq: redis options error", "err", err)
	}

	r := &Redis{
		prefix:          opt.prefix,
		UniversalClient: redis.NewUniversalClient(opt.options),
	}
	if opt.prefix != "" {
		r.AddHook(NewPrefixHook(opt.prefix))
	}
	return r
}

// BatchDelete
// @Description: 批量删除
// @receiver r
// @param ctx
// @param pattern 匹配模式，如foo* 表示匹配foo开头的所有key
// @param batchSize 单次批量删除的数量
// @return int64
// @return error
func (r *Redis) BatchDelete(ctx context.Context, pattern string, batchSize ...int64) (int64, error) {
	batchSize = append(batchSize, 1000)
	var cursor uint64
	var keys []string
	var total int64
	for {
		var err error
		keys, cursor, err = r.Scan(ctx, cursor, pattern, batchSize[0]).Result()
		if err != nil {
			return 0, fmt.Errorf("scan failed: %w", err)
		}
		if len(keys) > 0 {
			count, err := r.Del(ctx, keys...).Result()
			if err != nil {
				return 0, fmt.Errorf("delete failed: %w", err)
			}
			total += count
		}
		if cursor == 0 {
			break
		}
	}
	return total, nil
}

func (r *Redis) FloatToZ(values map[string]float64) []redis.Z {
	var zs []redis.Z
	for member, score := range values {
		zs = append(zs, redis.Z{
			Score:  score,
			Member: member,
		})
	}
	return zs
}
