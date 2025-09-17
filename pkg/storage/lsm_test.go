package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLSMBasicOperations 测试基本操作
func TestLSMBasicOperations(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	
	lsm, err := NewLSM(dir)
	if err != nil {
		t.Fatalf("Failed to create LSM: %v", err)
	}
	defer lsm.Close()
	
	// 测试 Put
	err = lsm.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}
	
	// 测试 Get
	value, found, err := lsm.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !found {
		t.Errorf("Expected to find key1")
	}
	if string(value) != "value1" {
		t.Errorf("Expected value1, got %s", string(value))
	}
	
	// 测试 Delete
	err = lsm.Delete([]byte("key1"))
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	
	// 验证删除
	_, found, err = lsm.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if found {
		t.Errorf("Should not find deleted key")
	}
}

// TestLSMUpdate 测试更新
func TestLSMUpdate(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	
	lsm, err := NewLSM(dir)
	if err != nil {
		t.Fatalf("Failed to create LSM: %v", err)
	}
	defer lsm.Close()
	
	// 插入
	lsm.Put([]byte("key"), []byte("value1"))
	
	// 更新
	lsm.Put([]byte("key"), []byte("value2"))
	
	// 验证
	value, found, _ := lsm.Get([]byte("key"))
	if !found {
		t.Errorf("Expected to find key")
	}
	if string(value) != "value2" {
		t.Errorf("Expected value2, got %s", string(value))
	}
}

// TestLSMFlush 测试刷盘
func TestLSMFlush(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	
	lsm, err := NewLSM(dir)
	if err != nil {
		t.Fatalf("Failed to create LSM: %v", err)
	}
	defer lsm.Close()
	
	// 写入大量数据触发刷盘
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		value := []byte(fmt.Sprintf("value-%06d", i))
		err := lsm.Put(key, value)
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}
	}
	
	// 等待刷盘完成
	time.Sleep(2 * time.Second)
	
	// 验证数据
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		value, found, err := lsm.Get(key)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		if !found {
			t.Errorf("Expected to find key-%06d", i)
		}
		expected := fmt.Sprintf("value-%06d", i)
		if string(value) != expected {
			t.Errorf("Expected %s, got %s", expected, string(value))
		}
	}
}

// TestLSMRecovery 测试崩溃恢复
func TestLSMRecovery(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	
	// 第一次：写入数据
	lsm1, err := NewLSM(dir)
	if err != nil {
		t.Fatalf("Failed to create LSM: %v", err)
	}
	
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		lsm1.Put(key, value)
	}
	
	lsm1.Close()
	
	// 第二次：恢复并验证
	lsm2, err := NewLSM(dir)
	if err != nil {
		t.Fatalf("Failed to recover LSM: %v", err)
	}
	defer lsm2.Close()
	
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value, found, err := lsm2.Get(key)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		if !found {
			t.Errorf("Expected to find key-%d after recovery", i)
		}
		expected := fmt.Sprintf("value-%d", i)
		if string(value) != expected {
			t.Errorf("Expected %s, got %s", expected, string(value))
		}
	}
}

// TestLSMConcurrent 测试并发操作
func TestLSMConcurrent(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	
	lsm, err := NewLSM(dir)
	if err != nil {
		t.Fatalf("Failed to create LSM: %v", err)
	}
	defer lsm.Close()
	
	// 并发写入
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				lsm.Put(key, value)
			}
			done <- true
		}(i)
	}
	
	// 等待完成
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// 验证数据
	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			key := []byte(fmt.Sprintf("key-%d-%d", i, j))
			_, found, err := lsm.Get(key)
			if err != nil {
				t.Errorf("Get failed: %v", err)
			}
			if !found {
				t.Errorf("Expected to find key-%d-%d", i, j)
			}
		}
	}
}

// TestLSMEmptyKey 测试空键
func TestLSMEmptyKey(t *testing.T) {
	dir := createTestDir(t)
	defer os.RemoveAll(dir)
	
	lsm, err := NewLSM(dir)
	if err != nil {
		t.Fatalf("Failed to create LSM: %v", err)
	}
	defer lsm.Close()
	
	// 测试空键
	err = lsm.Put([]byte(""), []byte("value"))
	if err == nil {
		t.Errorf("Expected error for empty key")
	}
}

// BenchmarkLSMPut 性能测试：写入
func BenchmarkLSMPut(b *testing.B) {
	dir := createTestDir(b)
	defer os.RemoveAll(dir)
	
	lsm, err := NewLSM(dir)
	if err != nil {
		b.Fatalf("Failed to create LSM: %v", err)
	}
	defer lsm.Close()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		lsm.Put(key, value)
	}
}

// BenchmarkLSMGet 性能测试：读取
func BenchmarkLSMGet(b *testing.B) {
	dir := createTestDir(b)
	defer os.RemoveAll(dir)
	
	lsm, err := NewLSM(dir)
	if err != nil {
		b.Fatalf("Failed to create LSM: %v", err)
	}
	defer lsm.Close()
	
	// 预填充数据
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		lsm.Put(key, value)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i%10000))
		lsm.Get(key)
	}
}

// 辅助函数：创建测试目录
func createTestDir(t testing.TB) string {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("lsm-test-%d", time.Now().UnixNano()))
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test dir: %v", err)
	}
	return dir
}
