# 存储引擎文档

## LSM-Tree 存储引擎

### 概述

LSM-Tree (Log-Structured Merge-Tree) 是一种针对写密集型工作负载优化的数据结构。

### 核心思想

1. **写入优化**：所有写入先进入内存，批量刷盘
2. **顺序写入**：磁盘写入都是顺序的，避免随机 I/O
3. **后台压缩**：异步合并和去重，不阻塞前台操作

## 组件详解

### 1. Memtable (内存表)

#### 实现：SkipList

```go
type SkipList struct {
    head      *SkipListNode
    maxLevel  int
    level     int
    size      int64
}
```

#### 特性

- **有序存储**：键按字典序排列
- **快速查找**：O(log N) 时间复杂度
- **并发安全**：读写锁保护
- **大小限制**：默认 64MB

#### 操作

**Put 操作**：
```go
func (sl *SkipList) Put(key, value []byte, isTombstone bool) {
    // 1. 查找插入位置
    // 2. 创建新节点
    // 3. 更新指针
    // 4. 更新大小
}
```

**Get 操作**：
```go
func (sl *SkipList) Get(key []byte) ([]byte, bool, bool) {
    // 1. 从顶层开始查找
    // 2. 向下移动直到找到或确定不存在
    // 3. 返回值和墓碑标记
}
```

### 2. WAL (Write-Ahead Log)

#### 格式

```
┌─────────────────────────────────────┐
│ Entry 1                             │
│  ├─ IsTombstone (1 byte)            │
│  ├─ KeyLen (4 bytes)                │
│  ├─ ValueLen (4 bytes)              │
│  ├─ Key (KeyLen bytes)              │
│  └─ Value (ValueLen bytes)          │
├─────────────────────────────────────┤
│ Entry 2                             │
│  ...                                │
└─────────────────────────────────────┘
```

#### 操作

**Write 操作**：
```go
func (wal *WAL) Write(key, value []byte, isTombstone bool) error {
    // 1. 编码条目
    // 2. 写入文件
    // 3. 可选：立即同步到磁盘
}
```

**Replay 操作**：
```go
func (wal *WAL) Replay(callback func(key, value []byte, isTombstone bool) error) error {
    // 1. 从头读取文件
    // 2. 解码每个条目
    // 3. 调用回调函数
}
```

#### WAL 轮转

当 Memtable 满时：
1. 关闭当前 WAL
2. 重命名为 `wal_timestamp.log`
3. 创建新的 `wal.log`
4. Memtable 刷盘后删除旧 WAL

### 3. SSTable (Sorted String Table)

#### 文件结构

```
┌─────────────────────────────────────┐
│ Data Block 1 (4KB)                  │
│  ├─ Entry 1                         │
│  ├─ Entry 2                         │
│  └─ ...                             │
├─────────────────────────────────────┤
│ Data Block 2 (4KB)                  │
│  └─ ...                             │
├─────────────────────────────────────┤
│ Index Block                         │
│  ├─ Block 1: offset, length         │
│  ├─ Block 2: offset, length         │
│  └─ ...                             │
├─────────────────────────────────────┤
│ Footer                              │
│  ├─ Index Block Offset (8 bytes)    │
│  ├─ Index Block Length (8 bytes)    │
│  ├─ Min Key Length (4 bytes)        │
│  ├─ Min Key                         │
│  ├─ Max Key Length (4 bytes)        │
│  └─ Max Key                         │
└─────────────────────────────────────┘
```

#### Writer

```go
type Writer struct {
    file        *os.File
    blockWriter *bytes.Buffer
    index       []BlockInfo
    minKey      []byte
    maxKey      []byte
}
```

**写入流程**：
1. 写入条目到当前 Block
2. Block 满时（4KB）刷新到文件
3. 记录 Block 的偏移和长度
4. 最后写入 Index Block 和 Footer

#### Reader

```go
type Reader struct {
    file    *os.File
    index   []BlockInfo
    minKey  []byte
    maxKey  []byte
}
```

**读取流程**：
1. 检查键是否在 [minKey, maxKey] 范围内
2. 遍历所有 Data Block
3. 在 Block 内查找键
4. 返回值和墓碑标记

### 4. Compaction (压缩)

#### 触发条件

- L0 文件数 >= 4
- L_N 层大小超过目标大小

#### 压缩算法

**多路归并排序**：
```go
func (lsm *LSM) runCompaction(task *CompactionTask) ([]*sstable.Reader, error) {
    // 1. 为所有输入文件创建迭代器
    iters := createIterators(task.InputFiles)
    
    // 2. 创建最小堆
    heap := newCompactionHeap(iters)
    
    // 3. 归并循环
    for heap.Len() > 0 {
        // 3a. 弹出最小键（最新版本）
        item := heap.PopAndRefill()
        
        // 3b. 去重：丢弃所有旧版本
        skipDuplicates(heap, item.key)
        
        // 3c. 过滤墓碑
        if item.isTombstone {
            continue
        }
        
        // 3d. 写入输出文件
        outputWriter.Write(item.key, item.value)
    }
    
    return newReaders, nil
}
```

#### 去重逻辑

```go
// 堆的 Less 函数保证最新版本优先
func (h *compactionHeap) Less(i, j int) bool {
    cmp := bytes.Compare(h.items[i].key, h.items[j].key)
    if cmp != 0 {
        return cmp < 0
    }
    // 相同键时，iteratorIndex 大的（更新的）优先
    return h.items[i].iteratorIndex > h.items[j].iteratorIndex
}
```

#### 输出切分

当输出文件超过 64MB 时：
1. 关闭当前文件
2. 创建新文件
3. 继续写入

### 5. Level Manager (层级管理)

#### 层级结构

```go
type LevelManager struct {
    levels      [][]*sstable.Reader  // 7 层
    nextFileNum int                   // 文件序号
}
```

#### 查找压缩任务

**优先级**：
1. L0 → L1（最高优先级）
2. L1 → L2
3. L2 → L3
4. ...

**L0 → L1 压缩**：
```go
if len(lm.levels[0]) >= 4 {
    // 选择所有 L0 文件
    l0Files := lm.levels[0]
    
    // 找出 L0 的键范围
    minKey, maxKey := findKeyRange(l0Files)
    
    // 选择 L1 中重叠的文件
    l1Files := lm.findOverlappingFiles(1, minKey, maxKey)
    
    return &CompactionTask{
        Level:       0,
        OutputLevel: 1,
        InputFiles:  append(l1Files, l0Files...),
    }
}
```

**L_N → L_N+1 压缩**：
```go
if lm.levelTotalSize(level) >= lm.levelTargetSize(level) {
    // 选择一个文件
    fileToCompact := lm.levels[level][0]
    
    // 找出下一层重叠的文件
    nextLevelFiles := lm.findOverlappingFiles(
        level+1,
        fileToCompact.MinKey(),
        fileToCompact.MaxKey(),
    )
    
    return &CompactionTask{
        Level:       level,
        OutputLevel: level + 1,
        InputFiles:  append(nextLevelFiles, fileToCompact),
    }
}
```

#### 应用压缩结果

```go
func (lm *LevelManager) ApplyCompactionResult(task *CompactionTask, newReaders []*sstable.Reader) {
    // 1. 从输入层移除旧文件
    lm.levels[task.Level] = removeReaders(lm.levels[task.Level], task.InputFiles)
    
    // 2. 添加新文件到输出层
    lm.levels[task.OutputLevel] = append(lm.levels[task.OutputLevel], newReaders...)
    
    // 3. 对输出层排序（L1+ 需要有序）
    if task.OutputLevel > 0 {
        sort.Slice(lm.levels[task.OutputLevel], func(i, j int) bool {
            return bytes.Compare(
                lm.levels[task.OutputLevel][i].MinKey(),
                lm.levels[task.OutputLevel][j].MinKey(),
            ) < 0
        })
    }
    
    // 4. 返回需要删除的旧文件
    return task.InputFiles
}
```

## 并发控制

### 读写锁

```go
type LSM struct {
    lock sync.RWMutex
    // ...
}

// 写操作：获取写锁
func (lsm *LSM) Put(key, value []byte) error {
    lsm.lock.Lock()
    defer lsm.lock.Unlock()
    // ...
}

// 读操作：获取读锁
func (lsm *LSM) Get(key []byte) ([]byte, bool, error) {
    lsm.lock.RLock()
    defer lsm.lock.RUnlock()
    // ...
}
```

### 快照隔离

```go
// 获取快照
snapshot := lsm.levelManager.GetSnapshot()
defer snapshot.Close()

// 在快照上查询
for level := 0; level < maxLevels; level++ {
    readers := snapshot.levels[level]
    // 查询...
}
```

## 性能优化

### 1. 批量写入

```go
// 避免：多次单独写入
for k, v := range items {
    lsm.Put(k, v)
}

// 推荐：批量写入
batch := lsm.NewBatch()
for k, v := range items {
    batch.Put(k, v)
}
batch.Commit()
```

### 2. Bloom Filter

```go
// 在 SSTable Reader 中添加 Bloom Filter
type Reader struct {
    bloomFilter *BloomFilter
    // ...
}

// Get 操作先检查 Bloom Filter
func (r *Reader) Get(key []byte) ([]byte, bool, bool, error) {
    if !r.bloomFilter.MayContain(key) {
        return nil, false, false, nil  // 快速返回
    }
    // 继续查找...
}
```

### 3. Block Cache

```go
// 缓存热点 Block
type BlockCache struct {
    cache *lru.Cache
}

func (r *Reader) readBlock(index int) ([]byte, error) {
    // 先查缓存
    if data, ok := r.blockCache.Get(index); ok {
        return data, nil
    }
    
    // 从磁盘读取
    data := r.readFromDisk(index)
    r.blockCache.Add(index, data)
    return data, nil
}
```

## 故障场景

### 1. 崩溃恢复

```
启动流程：
1. 打开 WAL
2. 回放所有条目到 Memtable
3. 加载所有 SSTable
4. 继续服务
```

### 2. WAL 损坏

```
处理：
1. 读取到损坏位置停止
2. 丢弃损坏的条目
3. 记录警告日志
4. 继续启动
```

### 3. SSTable 损坏

```
处理：
1. 跳过损坏的文件
2. 记录错误日志
3. 继续服务
4. 后台压缩时删除损坏文件
```

## 监控和调试

### 关键指标

```go
type Metrics struct {
    // 写入指标
    PutCount    int64
    PutLatency  time.Duration
    
    // 读取指标
    GetCount    int64
    GetLatency  time.Duration
    CacheHits   int64
    CacheMisses int64
    
    // 压缩指标
    CompactionCount    int64
    CompactionDuration time.Duration
    
    // 存储指标
    MemtableSize int64
    L0FileCount  int
    TotalSize    int64
}
```

### 调试工具

```bash
# 查看数据目录
ls -lh data/
# wal.log - 当前 WAL
# 0001.sst, 0002.sst - SSTable 文件

# 查看 SSTable 内容
./bin/kv-tool dump data/0001.sst

# 检查数据完整性
./bin/kv-tool check data/
```

## 参考实现

- [LevelDB](https://github.com/google/leveldb)
- [RocksDB](https://github.com/facebook/rocksdb)
- [Badger](https://github.com/dgraph-io/badger)
