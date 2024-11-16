package mcache

import (
	"sync"
	"time"
)

type ICache interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	Delete(key string)
	Len() int
	Cap() int
	Keys() []string
	Values() []any
	Items() map[string]any
	Clear()
	LList() []any
	RList() []any
}

func New(cap int) ICache {
	return newCache(cap)
}

type node struct {
	key       string
	value     any
	pre       *node
	next      *node
	visitedAt time.Time
}

func newNode(key string, value any) *node {
	return &node{
		key:       key,
		value:     value,
		visitedAt: time.Now(),
	}
}

type cache struct {
	dataPtr map[string]*node
	head    *node
	tail    *node
	cap     int
	sync.Mutex
}

func newCache(cap int) *cache {
	if cap <= 0 {
		panic("cache capacity must be greater than 0")
	}
	return &cache{
		dataPtr: make(map[string]*node, cap),
		cap:     cap,
	}
}

func (c *cache) Get(key string) (any, bool) {
	c.Lock()
	defer c.Unlock()
	n := c.dataPtr[key]
	if n == nil {
		return nil, false
	}
	n.visitedAt = time.Now()
	if n == c.head {
		return n.value, true
	}

	c.setNodeToFirst(n)

	return n.value, true
}

func (c *cache) Set(key string, value any) {
	c.Lock()
	defer c.Unlock()

	if n := c.dataPtr[key]; n != nil {
		n.value = value
		n.visitedAt = time.Now()
		if n != c.head {
			c.setNodeToFirst(n)
		}
		return
	}

	if len(c.dataPtr) >= c.cap {
		delete(c.dataPtr, c.tail.key)
		c.tail = c.tail.pre
		if c.tail != nil {
			c.tail.next = nil
		}
	}
	n := newNode(key, value)
	c.dataPtr[key] = n
	if c.head == nil {
		c.head = n
		c.tail = n
	} else {
		c.head.pre = n
		n.next = c.head
		c.head = n
	}
}

func (c *cache) setNodeToFirst(n *node) {
	pre := n.pre
	next := n.next

	n.pre = nil
	n.next = c.head
	c.head.pre = n
	c.head = n

	if pre != nil {
		pre.next = next
	}
	if next != nil {
		next.pre = pre
	}

	if n == c.tail {
		if pre != nil {
			c.tail = pre
		} else {
			c.tail = c.head
		}
	}
}

func (c *cache) Delete(key string) {
	c.Lock()
	defer c.Unlock()

	n := c.dataPtr[key]
	if n == nil {
		return
	}
	delete(c.dataPtr, key)

	if n.pre != nil {
		n.pre.next = n.next
	} else {
		c.head = n.next
	}
	if n.next != nil {
		n.next.pre = n.pre
	} else {
		c.tail = n.pre
	}
}

func (c *cache) LList() []any {
	c.Lock()
	defer c.Unlock()
	list := make([]any, 0, len(c.dataPtr))
	for n := c.head; n != nil; n = n.next {
		list = append(list, n.value)
	}
	return list
}

func (c *cache) RList() []any {
	c.Lock()
	defer c.Unlock()
	list := make([]any, 0, len(c.dataPtr))
	for n := c.tail; n != nil; n = n.pre {
		list = append(list, n.value)
	}
	return list
}

func (c *cache) Items() map[string]any {
	c.Lock()
	defer c.Unlock()
	items := make(map[string]any, len(c.dataPtr))
	for k, n := range c.dataPtr {
		items[k] = n.value
	}
	return items
}

func (c *cache) Keys() []string {
	c.Lock()
	defer c.Unlock()
	keys := make([]string, 0, len(c.dataPtr))
	for k := range c.dataPtr {
		keys = append(keys, k)
	}
	return keys
}

func (c *cache) Values() []any {
	c.Lock()
	defer c.Unlock()
	values := make([]any, 0, len(c.dataPtr))
	for _, n := range c.dataPtr {
		values = append(values, n.value)
	}
	return values
}

func (c *cache) Len() int {
	c.Lock()
	defer c.Unlock()
	return len(c.dataPtr)
}

func (c *cache) Cap() int {
	c.Lock()
	defer c.Unlock()
	return c.cap
}

func (c *cache) Clear() {
	c.Lock()
	defer c.Unlock()
	c.dataPtr = make(map[string]*node, c.cap)
	c.head = nil
	c.tail = nil
}

