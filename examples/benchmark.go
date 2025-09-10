package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
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

	fmt.Println("=== KV Storage Benchmark ===\n")

	// 写入性能测试
	fmt.Println("1. Write Performance Test")
	benchmarkWrite(cli, 10000)

	// 读取性能测试
	fmt.Println("\n2. Read Performance Test")
	benchmarkRead(cli, 10000)

	// 混合负载测试
	fmt.Println("\n3. Mixed Workload Test")
	benchmarkMixed(cli, 10000)

	// 并发测试
	fmt.Println("\n4. Concurrent Test")
	benchmarkConcurrent(cli, 10000, 10)
}

// 写入性能测试
func benchmarkWrite(cli *client.Client, count int) {
	ctx := context.Background()
	start := time.Now()

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key:%d", i))
		value := []byte(fmt.Sprintf("value:%d", i))

		err := cli.Put(ctx, key, value)
		if err != nil {
			log.Printf("Put failed: %v", err)
			return
		}
	}

	duration := time.Since(start)
	qps := float64(count) / duration.Seconds()

	fmt.Printf("  Operations: %d\n", count)
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf("  QPS: %.2f ops/s\n", qps)
	fmt.Printf("  Latency: %.2f ms/op\n", duration.Seconds()*1000/float64(count))
}

// 读取性能测试
func benchmarkRead(cli *client.Client, count int) {
	ctx := context.Background()

	// 先写入数据
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key:%d", i))
		value := []byte(fmt.Sprintf("value:%d", i))
		cli.Put(ctx, key, value)
	}

	// 读取测试
	start := time.Now()
	hits := 0

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key:%d", i))

		_, found, err := cli.Get(ctx, key)
		if err != nil {
			log.Printf("Get failed: %v", err)
			return
		}
		if found {
			hits++
		}
	}

	duration := time.Since(start)
	qps := float64(count) / duration.Seconds()
	hitRate := float64(hits) / float64(count) * 100

	fmt.Printf("  Operations: %d\n", count)
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf("  QPS: %.2f ops/s\n", qps)
	fmt.Printf("  Latency: %.2f ms/op\n", duration.Seconds()*1000/float64(count))
	fmt.Printf("  Hit Rate: %.2f%%\n", hitRate)
}

// 混合负载测试
func benchmarkMixed(cli *client.Client, count int) {
	ctx := context.Background()

	// 先写入一些数据
	for i := 0; i < count/2; i++ {
		key := []byte(fmt.Sprintf("key:%d", i))
		value := []byte(fmt.Sprintf("value:%d", i))
		cli.Put(ctx, key, value)
	}

	// 混合读写测试 (70% 读, 30% 写)
	start := time.Now()
	reads := 0
	writes := 0

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key:%d", i%count))

		if i%10 < 7 {
			// 读操作
			_, _, err := cli.Get(ctx, key)
			if err != nil {
				log.Printf("Get failed: %v", err)
				return
			}
			reads++
		} else {
			// 写操作
			value := []byte(fmt.Sprintf("value:%d", i))
			err := cli.Put(ctx, key, value)
			if err != nil {
				log.Printf("Put failed: %v", err)
				return
			}
			writes++
		}
	}

	duration := time.Since(start)
	qps := float64(count) / duration.Seconds()

	fmt.Printf("  Operations: %d (Reads: %d, Writes: %d)\n", count, reads, writes)
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf("  QPS: %.2f ops/s\n", qps)
	fmt.Printf("  Latency: %.2f ms/op\n", duration.Seconds()*1000/float64(count))
}

// 并发测试
func benchmarkConcurrent(cli *client.Client, count int, concurrency int) {
	ctx := context.Background()
	var wg sync.WaitGroup
	var successCount atomic.Int64
	var errorCount atomic.Int64

	opsPerWorker := count / concurrency

	start := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < opsPerWorker; i++ {
				key := []byte(fmt.Sprintf("worker:%d:key:%d", workerID, i))
				value := []byte(fmt.Sprintf("value:%d", i))

				err := cli.Put(ctx, key, value)
				if err != nil {
					errorCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)

	totalOps := successCount.Load() + errorCount.Load()
	qps := float64(totalOps) / duration.Seconds()

	fmt.Printf("  Workers: %d\n", concurrency)
	fmt.Printf("  Operations: %d\n", totalOps)
	fmt.Printf("  Success: %d\n", successCount.Load())
	fmt.Printf("  Errors: %d\n", errorCount.Load())
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf("  QPS: %.2f ops/s\n", qps)
	fmt.Printf("  Latency: %.2f ms/op\n", duration.Seconds()*1000/float64(totalOps))
}
