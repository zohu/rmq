package rmq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestCache_Set(t *testing.T) {
	ctx := context.Background()
	key := "test-key"
	value := "test-value"
	ttl := 15 * time.Minute
	cache.Set(ctx, key, value, ttl)
}

func TestCache_Get_HitL1(t *testing.T) {
	key := "test-key"
	value := "test-value"
	v, err := cache.Get(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, value, v)
}

func TestCache_Get_MissL1_HitL2(t *testing.T) {
	ctx := context.Background()
	key := "test-key"
	value := "test-value"
	v, err := cache.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, v)
}

func TestCache_Get_MissL1_L2KeyNotFound(t *testing.T) {
	ctx := context.Background()
	key := "not-exist"
	_, err := cache.Get(ctx, key)
	assert.Equal(t, redis.Nil, err)
}

func TestCache_Del(t *testing.T) {
	ctx := context.Background()
	key := "test-key"
	err := cache.Del(ctx, key)
	assert.NoError(t, err)
}
