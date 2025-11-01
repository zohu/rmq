package rmq

import (
	"errors"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

type redisOptions struct {
	prefix  Prefix
	options *redis.UniversalOptions
}

func (o *redisOptions) validate() error {
	if o.options == nil {
		return errors.New("redis options is nil")
	}
	if o.options.Addrs == nil || len(o.options.Addrs) == 0 {
		return errors.New("redis options is empty")
	}
	if o.options.MaintNotificationsConfig == nil {
		o.options.MaintNotificationsConfig = &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		}
	}
	if o.prefix == "" {
		o.prefix = "rmq"
	}
	return nil
}

type RedisOption func(*redisOptions)

func WithRedisOptions(redis *redis.UniversalOptions) RedisOption {
	return func(o *redisOptions) {
		o.options = redis
	}
}
func WithRedisPrefix(prefix string) RedisOption {
	return func(o *redisOptions) {
		o.prefix = Prefix(prefix)
	}
}
