package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ph0m1/kv/pkg/client"
)

func main() {
	// 命令行参数
	leaderAddr := flag.String("leader", "localhost:8001", "Leader address")
	numOps := flag.Int("ops", 100, "Number of operations")
	flag.Parse()

	// 创建客户端
	cli, err := client.NewClient(*leaderAddr, nil)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	fmt.Printf("Connecting to Raft cluster leader at %s\n", *leaderAddr)
	fmt.Printf("Performing %d operations...\n\n", *numOps)

	// 1. 写入数据
	fmt.Println("=== Writing data ===")
	start := time.Now()
	for i := 0; i < *numOps; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		
		if err := cli.Put(ctx, key, value); err != nil {
			log.Printf("Put failed: %v", err)
			continue
		}
		
		if (i+1)%10 == 0 {
			fmt.Printf("Written %d keys\n", i+1)
		}
	}
	writeDuration := time.Since(start)
	fmt.Printf("Write completed: %d ops in %v (%.2f ops/s)\n\n",
		*numOps, writeDuration, float64(*numOps)/writeDuration.Seconds())

	// 2. 读取数据
	fmt.Println("=== Reading data ===")
	start = time.Now()
	successCount := 0
	for i := 0; i < *numOps; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		
		value, found, err := cli.Get(ctx, key)
		if err != nil {
			log.Printf("Get failed: %v", err)
			continue
		}
		
		if !found {
			log.Printf("Key not found: %s", key)
			continue
		}
		
		expected := fmt.Sprintf("value-%d", i)
		if string(value) != expected {
			log.Printf("Value mismatch: expected %s, got %s", expected, value)
			continue
		}
		
		successCount++
		if (i+1)%10 == 0 {
			fmt.Printf("Read %d keys\n", i+1)
		}
	}
	readDuration := time.Since(start)
	fmt.Printf("Read completed: %d/%d successful in %v (%.2f ops/s)\n\n",
		successCount, *numOps, readDuration, float64(*numOps)/readDuration.Seconds())

	// 3. 更新数据
	fmt.Println("=== Updating data ===")
	start = time.Now()
	for i := 0; i < *numOps/10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("updated-value-%d", i))
		
		if err := cli.Put(ctx, key, value); err != nil {
			log.Printf("Update failed: %v", err)
			continue
		}
	}
	updateDuration := time.Since(start)
	fmt.Printf("Update completed: %d ops in %v\n\n", *numOps/10, updateDuration)

	// 4. 删除数据
	fmt.Println("=== Deleting data ===")
	start = time.Now()
	for i := 0; i < *numOps/10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		
		if err := cli.Delete(ctx, key); err != nil {
			log.Printf("Delete failed: %v", err)
			continue
		}
	}
	deleteDuration := time.Since(start)
	fmt.Printf("Delete completed: %d ops in %v\n\n", *numOps/10, deleteDuration)

	// 5. 验证删除
	fmt.Println("=== Verifying deletions ===")
	for i := 0; i < *numOps/10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		
		_, found, err := cli.Get(ctx, key)
		if err != nil {
			log.Printf("Get failed: %v", err)
			continue
		}
		
		if found {
			log.Printf("Key should be deleted but still exists: %s", key)
		}
	}
	fmt.Println("Verification completed")

	fmt.Println("\n=== Summary ===")
	fmt.Printf("Total operations: %d\n", *numOps)
	fmt.Printf("Write throughput: %.2f ops/s\n", float64(*numOps)/writeDuration.Seconds())
	fmt.Printf("Read throughput: %.2f ops/s\n", float64(*numOps)/readDuration.Seconds())
	fmt.Printf("Success rate: %.2f%%\n", float64(successCount)/float64(*numOps)*100)
}
