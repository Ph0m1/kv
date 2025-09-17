package storage

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// TestSkipListBasicOperations 测试基本操作
func TestSkipListBasicOperations(t *testing.T) {
	sl := NewSkipList(12)
	
	// 测试 Put
	sl.Put([]byte("key1"), []byte("value1"), false)
	sl.Put([]byte("key2"), []byte("value2"), false)
	sl.Put([]byte("key3"), []byte("value3"), false)
	
	// 测试 Get
	value, isTombstone, found := sl.Get([]byte("key1"))
	if !found {
		t.Errorf("Expected to find key1")
	}
	if string(value) != "value1" {
		t.Errorf("Expected value1, got %s", string(value))
	}
	if isTombstone {
		t.Errorf("Expected non-tombstone")
	}
	
	// 测试不存在的键
	_, _, found = sl.Get([]byte("nonexistent"))
	if found {
		t.Errorf("Should not find nonexistent key")
	}
}

// TestSkipListUpdate 测试更新操作
func TestSkipListUpdate(t *testing.T) {
	sl := NewSkipList(12)
	
	// 插入
	sl.Put([]byte("key"), []byte("value1"), false)
	
	// 更新
	sl.Put([]byte("key"), []byte("value2"), false)
	
	// 验证
	value, _, found := sl.Get([]byte("key"))
	if !found {
		t.Errorf("Expected to find key")
	}
	if string(value) != "value2" {
		t.Errorf("Expected value2, got %s", string(value))
	}
}

// TestSkipListTombstone 测试墓碑标记
func TestSkipListTombstone(t *testing.T) {
	sl := NewSkipList(12)
	
	// 插入
	sl.Put([]byte("key"), []byte("value"), false)
	
	// 删除（墓碑）
	sl.Put([]byte("key"), nil, true)
	
	// 验证
	_, isTombstone, found := sl.Get([]byte("key"))
	if !found {
		t.Errorf("Expected to find key")
	}
	if !isTombstone {
		t.Errorf("Expected tombstone")
	}
}

// TestSkipListOrdering 测试排序
func TestSkipListOrdering(t *testing.T) {
	sl := NewSkipList(12)
	
	// 乱序插入
	keys := []string{"key5", "key1", "key3", "key2", "key4"}
	for _, key := range keys {
		sl.Put([]byte(key), []byte("value"), false)
	}
	
	// 验证顺序
	expected := []string{"key1", "key2", "key3", "key4", "key5"}
	current := sl.head.forward[0]
	i := 0
	
	for current != nil {
		if string(current.key) != expected[i] {
			t.Errorf("Expected %s at position %d, got %s", expected[i], i, string(current.key))
		}
		current = current.forward[0]
		i++
	}
}

// TestSkipListConcurrent 测试并发安全
func TestSkipListConcurrent(t *testing.T) {
	sl := NewSkipList(12)
	var wg sync.WaitGroup
	
	// 并发写入
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				sl.Put(key, value, false)
			}
		}(i)
	}
	
	// 并发读取
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				sl.Get(key)
			}
		}(i)
	}
	
	wg.Wait()
}

// TestSkipListSize 测试大小计算
func TestSkipListSize(t *testing.T) {
	sl := NewSkipList(12)
	
	initialSize := sl.ApproximateSize()
	if initialSize != 0 {
		t.Errorf("Expected initial size 0, got %d", initialSize)
	}
	
	// 插入数据
	sl.Put([]byte("key"), []byte("value"), false)
	
	newSize := sl.ApproximateSize()
	if newSize <= initialSize {
		t.Errorf("Expected size to increase")
	}
}

// BenchmarkSkipListPut 性能测试：写入
func BenchmarkSkipListPut(b *testing.B) {
	sl := NewSkipList(12)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		sl.Put(key, value, false)
	}
}

// BenchmarkSkipListGet 性能测试：读取
func BenchmarkSkipListGet(b *testing.B) {
	sl := NewSkipList(12)
	
	// 预填充数据
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		sl.Put(key, value, false)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", rand.Intn(10000)))
		sl.Get(key)
	}
}

// BenchmarkSkipListConcurrent 性能测试：并发
func BenchmarkSkipListConcurrent(b *testing.B) {
	sl := NewSkipList(12)
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("key-%d", i))
			value := []byte(fmt.Sprintf("value-%d", i))
			sl.Put(key, value, false)
			sl.Get(key)
			i++
		}
	})
}
