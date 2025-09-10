# 示例代码

这个目录包含了 KV 存储系统的使用示例。

## 示例列表

### 1. basic_usage.go
基本使用示例，包括：
- Put/Get/Delete 操作
- 批量操作
- 错误处理
- 超时控制

**运行方式**：
```bash
# 先启动服务器
./bin/kv-server

# 在另一个终端运行示例
go run examples/basic_usage.go
```

### 2. benchmark.go
性能测试示例，包括：
- 写入性能测试
- 读取性能测试
- 混合负载测试
- 并发测试

**运行方式**：
```bash
# 先启动服务器
./bin/kv-server

# 在另一个终端运行基准测试
go run examples/benchmark.go
```

## 注意事项

1. 运行示例前请确保服务器已启动
2. 示例会在服务器上创建测试数据
3. 基准测试可能需要几分钟时间
4. 建议在测试环境运行，避免影响生产数据

## 预期输出

### basic_usage.go
```
=== Example 1: Basic Operations ===
✓ Put: user:1001 = Alice
✓ Get: user:1001 = Alice
✓ Delete: user:1001
✓ Verified: user:1001 not found

=== Example 2: Batch Operations ===
✓ BatchPut: 3 items
✓ BatchGet: 3 items
  product:1 = Laptop
  product:2 = Mouse
  product:3 = Keyboard

=== Example 3: Error Handling ===
✓ Key not found (expected)
✓ Empty key error (expected): key cannot be empty

=== Example 4: Timeout Control ===
✓ Put with 1s timeout succeeded
✓ Get with timeout: value
```

### benchmark.go
```
=== KV Storage Benchmark ===

1. Write Performance Test
  Operations: 10000
  Duration: 2.5s
  QPS: 4000.00 ops/s
  Latency: 0.25 ms/op

2. Read Performance Test
  Operations: 10000
  Duration: 1.8s
  QPS: 5555.56 ops/s
  Latency: 0.18 ms/op
  Hit Rate: 100.00%

3. Mixed Workload Test
  Operations: 10000 (Reads: 7000, Writes: 3000)
  Duration: 2.2s
  QPS: 4545.45 ops/s
  Latency: 0.22 ms/op

4. Concurrent Test
  Workers: 10
  Operations: 10000
  Success: 10000
  Errors: 0
  Duration: 1.5s
  QPS: 6666.67 ops/s
  Latency: 0.15 ms/op
```

## 自定义测试

你可以修改示例代码来测试不同的场景：

```go
// 修改测试数据量
benchmarkWrite(cli, 100000)  // 10万次写入

// 修改并发数
benchmarkConcurrent(cli, 10000, 50)  // 50个并发worker

// 修改超时时间
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
```
