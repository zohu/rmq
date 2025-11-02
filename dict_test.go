package rmq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func init() {
	InitRedis(
		WithRedisPrefix("test"),
		WithRedisOptions(&redis.UniversalOptions{
			Addrs:      []string{"localhost:8011"},
			Password:   "JCFkQYex4f",
			ClientName: "rmq",
		}),
	)
	InitMemory(WithMemoryPrefix("test"))
	InitCache(Mem(), Rds())
}

// 模拟一个外部数据源查询函数
func fakeUserQuery(ctx context.Context, key string) string {
	// 模拟数据库或 API 查询
	time.Sleep(10 * time.Millisecond) // 模拟延迟
	if key == "user:123" {
		return "Alice"
	}
	if key == "user:456" {
		return "Bob"
	}
	return "" // not found
}

func TestDictIntegration(t *testing.T) {
	ctx := context.Background()
	dictName := DictName("user_dict")

	// 注册字典
	NewDict(dictName, fakeUserQuery, time.Minute*5)

	// 清理函数：测试结束后删除可能写入的 key
	cleanup := func(keys ...string) {
		for _, k := range keys {
			fullKey := dictName.String(k)
			_ = Rds().Del(ctx, fullKey).Err()
			_ = CacheL2().Del(ctx, fullKey)
		}
	}
	defer cleanup("user:123", "user:456", "user:999")

	t.Run("query valid key, cache miss then hit", func(t *testing.T) {
		// 第一次：缓存未命中，应调用 query
		val1, err := Dict(ctx, dictName, "user:123")
		assert.NoError(t, err)
		assert.Equal(t, "Alice", val1)

		// 第二次：应命中 Redis 缓存（Rds 或 CacheL2）
		val2, err := Dict(ctx, dictName, "user:123")
		assert.NoError(t, err)
		assert.Equal(t, "Alice", val2)
	})

	t.Run("query not found key", func(t *testing.T) {
		_, err := Dict(ctx, dictName, "user:999")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("empty key", func(t *testing.T) {
		_, err := Dict(ctx, dictName, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "key is empty")
	})

	t.Run("unregistered dict", func(t *testing.T) {
		_, err := Dict(ctx, DictName("nonexistent"), "key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "dict nonexistent not registered")
		assert.Contains(t, err.Error(), "NewDict")
	})

	t.Run("different keys return different values", func(t *testing.T) {
		val1, _ := Dict(ctx, dictName, "user:123")
		val2, _ := Dict(ctx, dictName, "user:456")
		assert.Equal(t, "Alice", val1)
		assert.Equal(t, "Bob", val2)
	})
}
