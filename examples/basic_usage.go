package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ph0m1/kv/pkg/client"
)

func main() {
	// 创建客户端
	cli, err := client.NewClient("localhost:9090", nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	// 示例1: 基本的 Put/Get/Delete
	fmt.Println("=== Example 1: Basic Operations ===")
	basicOperations(ctx, cli)

	// 示例2: 批量操作
	fmt.Println("\n=== Example 2: Batch Operations ===")
	batchOperations(ctx, cli)

	// 示例3: 错误处理
	fmt.Println("\n=== Example 3: Error Handling ===")
	errorHandling(ctx, cli)

	// 示例4: 超时控制
	fmt.Println("\n=== Example 4: Timeout Control ===")
	timeoutControl(cli)
}

// 基本操作示例
func basicOperations(ctx context.Context, cli *client.Client) {
	// Put
	err := cli.Put(ctx, []byte("user:1001"), []byte("Alice"))
	if err != nil {
		log.Printf("Put failed: %v", err)
		return
	}
	fmt.Println("✓ Put: user:1001 = Alice")

	// Get
	value, found, err := cli.Get(ctx, []byte("user:1001"))
	if err != nil {
		log.Printf("Get failed: %v", err)
		return
	}
	if found {
		fmt.Printf("✓ Get: user:1001 = %s\n", string(value))
	}

	// Delete
	err = cli.Delete(ctx, []byte("user:1001"))
	if err != nil {
		log.Printf("Delete failed: %v", err)
		return
	}
	fmt.Println("✓ Delete: user:1001")

	// 验证删除
	_, found, err = cli.Get(ctx, []byte("user:1001"))
	if err != nil {
		log.Printf("Get failed: %v", err)
		return
	}
	if !found {
		fmt.Println("✓ Verified: user:1001 not found")
	}
}

// 批量操作示例
func batchOperations(ctx context.Context, cli *client.Client) {
	// 批量写入
	items := map[string][]byte{
		"product:1": []byte("Laptop"),
		"product:2": []byte("Mouse"),
		"product:3": []byte("Keyboard"),
	}

	err := cli.BatchPut(ctx, items)
	if err != nil {
		log.Printf("BatchPut failed: %v", err)
		return
	}
	fmt.Printf("✓ BatchPut: %d items\n", len(items))

	// 批量读取
	keys := [][]byte{
		[]byte("product:1"),
		[]byte("product:2"),
		[]byte("product:3"),
	}

	result, err := cli.BatchGet(ctx, keys)
	if err != nil {
		log.Printf("BatchGet failed: %v", err)
		return
	}

	fmt.Printf("✓ BatchGet: %d items\n", len(result))
	for key, value := range result {
		fmt.Printf("  %s = %s\n", key, string(value))
	}
}

// 错误处理示例
func errorHandling(ctx context.Context, cli *client.Client) {
	// 读取不存在的键
	_, found, err := cli.Get(ctx, []byte("nonexistent"))
	if err != nil {
		log.Printf("Get failed: %v", err)
		return
	}
	if !found {
		fmt.Println("✓ Key not found (expected)")
	}

	// 空键错误
	err = cli.Put(ctx, []byte(""), []byte("value"))
	if err != nil {
		fmt.Printf("✓ Empty key error (expected): %v\n", err)
	}
}

// 超时控制示例
func timeoutControl(cli *client.Client) {
	// 设置1秒超时
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := cli.Put(ctx, []byte("timeout:test"), []byte("value"))
	if err != nil {
		log.Printf("Put with timeout failed: %v", err)
		return
	}
	fmt.Println("✓ Put with 1s timeout succeeded")

	// 读取
	value, found, err := cli.Get(ctx, []byte("timeout:test"))
	if err != nil {
		log.Printf("Get with timeout failed: %v", err)
		return
	}
	if found {
		fmt.Printf("✓ Get with timeout: %s\n", string(value))
	}
}
