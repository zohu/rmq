package rmq

import (
	"context"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
)

var prefixableCommands = map[string]bool{
	"GET": true, "SET": true, "EXISTS": true, "TYPE": true,
	"RPUSH": true, "LPOP": true, "RPOP": true, "LLEN": true, "LRANGE": true,
	"SADD": true, "SREM": true, "SISMEMBER": true, "SMEMBERS": true, "SCARD": true,
	"HSET": true, "HMSET": true, "HGET": true, "HGETALL": true,
	"ZADD": true, "ZRANGE": true, "ZRANGEBYSCORE": true, "ZREVRANGEBYSCORE": true, "ZREM": true,
	"INCR": true, "INCRBY": true, "INCRBYFLOAT": true,
	"WATCH": true, "MULTI": true, "EXEC": true, "EXPIRE": true,
}

type PrefixHook struct {
	prefix Prefix
}

func NewPrefixHook(prefix Prefix) *PrefixHook {
	return &PrefixHook{prefix: prefix}
}
func (h PrefixHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}
func (h PrefixHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.addPrefix(cmd)
		return next(ctx, cmd)
	}
}
func (h PrefixHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.addPrefix(cmd)
		}
		return next(ctx, cmds)
	}
}
func (h PrefixHook) addPrefix(cmd redis.Cmder) {
	args := cmd.Args()
	if len(args) <= 1 {
		return
	}
	name := cmd.Name()
	switch name {
	case "MGET", "DEL":
		for i := 1; i < len(args); i++ {
			if pattern, ok := toString(args[i]); ok {
				args[i] = h.withPrefix(pattern)
			}
		}
	case "MSET":
		for i := 1; i < len(args); i += 2 {
			if pattern, ok := toString(args[i]); ok {
				args[i] = h.withPrefix(pattern)
			}
		}
	case "SCAN":
		for i := 2; i < len(args); i += 2 {
			if matchKey, ok := toString(args[i]); ok && strings.EqualFold(matchKey, "match") {
				if pattern, ok := toString(args[i+1]); ok {
					args[i+1] = h.withPrefix(pattern)
				}
				break
			}
		}
	default:
		if h.canWithPrefix(name) {
			if pattern, ok := toString(args[1]); ok {
				args[1] = h.withPrefix(pattern)
			}
		}
	}
}
func (h PrefixHook) canWithPrefix(name string) bool {
	return prefixableCommands[name]
}
func (h PrefixHook) withPrefix(key string) string {
	if strings.HasPrefix(key, h.prefix.String()+":") {
		return key
	}
	return h.prefix.String(key)
}
func toString(v interface{}) (string, bool) {
	if s, ok := v.(string); ok {
		return s, true
	}
	return "", false
}
