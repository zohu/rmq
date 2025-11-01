package rmq

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestRedis_BatchDelete(t *testing.T) {
	InitRedis(
		WithRedisPrefix("test"),
		WithRedisOptions(&redis.UniversalOptions{
			Addrs:      []string{"localhost:8011"},
			Password:   "JCFkQYex4f",
			ClientName: "rmq",
		}),
	)
	Rds().Set(context.Background(), "a:1", "a", 0)
	Rds().Set(context.Background(), "a:2", "b", 0)
	Rds().Set(context.Background(), "a:3", "c", 0)
	count, err := Rds().BatchDelete(context.Background(), "a:*", 2)

	assert.Nil(t, err)
	assert.Equal(t, int64(3), count)
}
