package rmq

import (
	"testing"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/stretchr/testify/assert"
)

func TestMemory(t *testing.T) {
	t.Run("NewMemory with default options", func(t *testing.T) {
		m := NewMemory()
		defer m.Close()

		assert.NotNil(t, m)
		assert.NotNil(t, m.cache)
		assert.Equal(t, Prefix("rmq"), m.prefix)
	})

	t.Run("NewMemory with prefix", func(t *testing.T) {
		m := NewMemory(WithMemoryPrefix("test"))
		defer m.Close()

		assert.Equal(t, Prefix("test"), m.prefix)
	})

	t.Run("Set and Get", func(t *testing.T) {
		m := NewMemory()
		defer m.Close()

		m.Set("key1", "value1")
		val, ok := m.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", val)
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		m := NewMemory()
		defer m.Close()

		_, ok := m.Get("nonexistent")
		assert.False(t, ok)
	})

	t.Run("Delete key", func(t *testing.T) {
		m := NewMemory()
		defer m.Close()

		m.Set("key2", "value2")
		_, ok := m.Get("key2")
		assert.True(t, ok)

		m.Delete("key2")
		_, ok = m.Get("key2")
		assert.False(t, ok)
	})

	t.Run("Len", func(t *testing.T) {
		m := NewMemory()
		defer m.Close()

		assert.Equal(t, 0, m.Len())

		m.Set("k1", "v1")
		m.Set("k2", "v2")
		assert.Equal(t, 2, m.Len())
	})

	t.Run("Capacity", func(t *testing.T) {
		cfg := bigcache.DefaultConfig(time.Minute)
		cfg.Shards = 1
		cfg.MaxEntriesInWindow = 10
		cfg.MaxEntrySize = 100
		cfg.HardMaxCacheSize = 1

		m := NewMemory(WithMemoryOptions(&cfg))
		defer m.Close()

		assert.Greater(t, m.Capacity(), 0)
	})

	t.Run("Range", func(t *testing.T) {
		m := NewMemory(WithMemoryPrefix("p"))
		defer m.Close()

		m.Set("a", "1")
		m.Set("b", "2")

		keys := make(map[string]string)
		m.Range(func(key, value string) {
			keys[key] = value
		})

		assert.Equal(t, map[string]string{"a": "1", "b": "2"}, keys)
	})

	t.Run("Stats", func(t *testing.T) {
		m := NewMemory()
		defer m.Close()

		stats := m.Stats()
		assert.NotNil(t, stats)
	})

	t.Run("KeyMetadata", func(t *testing.T) {
		m := NewMemory()
		defer m.Close()

		m.Set("metaKey", "metaVal")
		meta := m.KeyMetadata("metaKey")
		assert.NotNil(t, meta)
	})

	t.Run("appendPrefix and removePrefix", func(t *testing.T) {
		m := &Memory{prefix: "pre"}

		keyWithPrefix := m.appendPrefix("key")
		assert.Equal(t, "pre:key", keyWithPrefix)

		originalKey := m.removePrefix(keyWithPrefix)
		assert.Equal(t, "key", originalKey)

		mNoPrefix := &Memory{prefix: ""}
		assert.Equal(t, "key", mNoPrefix.appendPrefix("key"))
		assert.Equal(t, "key", mNoPrefix.removePrefix("key"))
	})

	t.Run("ttl", func(t *testing.T) {
		m := NewMemory()
		defer m.Close()
		m.Set("key_e", "value", time.Second)
		v, _ := m.Get("key_e")
		assert.Equal(t, "value", v)
		time.Sleep(time.Second * 2)
		v, _ = m.Get("key_e")
		assert.Equal(t, "", v)
	})
}
