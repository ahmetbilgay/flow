package n8n

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync"
)

// poolCache keeps a bounded number of connection pools (databases, Redis)
// and closes the least recently used one when full.
type poolCache struct {
	mu    sync.Mutex
	max   int
	order []string
	items map[string]io.Closer
}

func newPoolCache(max int) *poolCache {
	return &poolCache{max: max, items: map[string]io.Closer{}}
}

// get returns the pool for key, creating it with mk when missing.
func (c *poolCache) get(key string, mk func() (io.Closer, error)) (io.Closer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.items[key]; ok {
		c.touch(key)
		return v, nil
	}
	v, err := mk()
	if err != nil {
		return nil, err
	}
	c.items[key] = v
	c.order = append(c.order, key)
	for len(c.order) > c.max {
		old := c.order[0]
		c.order = c.order[1:]
		if p, ok := c.items[old]; ok {
			_ = p.Close()
			delete(c.items, old)
		}
	}
	return v, nil
}

func (c *poolCache) touch(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(append(c.order[:i:i], c.order[i+1:]...), key)
			return
		}
	}
}

// secretKey hashes secrets so they can be part of cache keys without being
// kept in plain text.
func secretKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
