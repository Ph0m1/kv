package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ph0m1/kv/storage/sstable"
)

const (
	// Memtable 达到 64MB 时刷写
	defaultMemtableSizeLimit = 64 * 1024 * 1024
	// WAL 文件名
	walFileName = "wal.log"
)

// flushTask 封装了刷写任务所需的所有信息
type flushTask struct {
	mem *SkipList
	wal *WAL // 这个是需要被清理的 *旧* WAL
}

// LSM 是存储引擎的主结构
type LSM struct {
	// Memtables
	activeMemtable    *SkipList
	immutableMemtable *SkipList // 正在等待刷写的 Memtable

	// WAL
	wal *WAL

	// L0 SSTables
	// L0 层文件允许键重叠，所以用切片
	// !! 关键: 索引 0 是最旧的, len-1 是最新的
	// l0SSTables []*sstable.Reader
	levelManager *LevelManager

	// 引擎的主锁
	lock sync.RWMutex

	// 刷写控制
	flushChan chan flushTask // 通知 flushWorker 工作的通道
	flushDone chan struct{}  // flushWorker 完成时的信号
	stopChan  chan struct{}  // 关闭引擎的信号

	// 引擎配置
	dirPath           string // 数据目录
	memtableSizeLimit int64
}

// NewLSM 创建或打开一个 LSM 存储引擎
func NewLSM(dirPath string) (*LSM, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	lsm := &LSM{
		dirPath:           dirPath,
		memtableSizeLimit: defaultMemtableSizeLimit,
		flushChan:         make(chan flushTask, 1), // 缓冲为1
		flushDone:         make(chan struct{}),
		stopChan:          make(chan struct{}),
	}

	// 打开 WAL
	walPath := filepath.Join(dirPath, walFileName)
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	lsm.wal = wal

	// 回放 WAL, 重建 Memtable
	lsm.activeMemtable = NewSkipList(defaultMaxLevel)
	err = lsm.wal.Replay(func(key, value []byte, isTombstone bool) error {
		lsm.activeMemtable.Put(key, value, isTombstone)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	// // 加载 L0 SSTables
	// // 我们需要一种方法来命名 SSTable 文件，例如 0001.sst, 0002.sst
	// files, err := ioutil.ReadDir(dirPath)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to read data dir: %w", err)
	// }

	// var sstFiles []string
	// for _, f := range files {
	// 	if strings.HasSuffix(f.Name(), ".sst") {
	// 		sstFiles = append(sstFiles, f.Name())
	// 	}
	// }
	// // 按文件名排序 (0001.sst, 0002.sst, ...)
	// sort.Strings(sstFiles)

	// for _, fileName := range sstFiles {
	// 	sstPath := filepath.Join(dirPath, fileName)
	// 	reader, err := sstable.NewReader(sstPath)
	// 	if err != nil {
	// 		// 文件可能损坏，需要处理
	// 		return nil, fmt.Errorf("failed to load sstable %s: %w", fileName, err)
	// 	}
	// 	lsm.l0SSTables = append(lsm.l0SSTables, reader)
	// }

	// 初始化 levelManager
	lm, err := NewLevelManager(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to init level manager: %w", err)
	}
	lsm.levelManager = lm

	// 启动后台刷写 Goroutine
	go lsm.flushWorker()

	return lsm, nil
}

// flushWorker 是一个常驻的后台 Goroutine
func (lsm *LSM) flushWorker() {
	for {
		select {
		case <-lsm.stopChan:
			return

		case task := <-lsm.flushChan: // <-- 接收 flushTask
			// 生成新文件名
			// ... (逻辑不变, 使用序号或时间戳命名 .sst) ...
			// lsm.lock.RLock()
			// newSSTNum := len(lsm.l0SSTables) + 1 // (这个命名逻辑我们稍后会改进)
			// lsm.lock.RUnlock()
			// newSSTPath := filepath.Join(
			// 	lsm.dirPath,
			// 	fmt.Sprintf("%04d.sst", newSSTNum),
			// )
			newSSTNum := lsm.levelManager.NextFileNumber()
			newSSTPath := filepath.Join(
				lsm.dirPath,
				fmt.Sprintf("%04d.sst", newSSTNum), // 保证 4 位补零
			)

			// 执行刷写
			writer, err := sstable.NewWriter(newSSTPath)
			if err != nil {
				// ... (错误处理) ...
				lsm.flushDone <- struct{}{}
				continue
			}

			iter := task.mem.NewIterator() // <-- 从 task 中获取 mem
			if err := writer.Flush(iter); err != nil {
				// ... (错误处理) ...
				lsm.flushDone <- struct{}{}
				continue
			}
			writer.Close()

			// 3. 打开新文件的 Reader
			reader, err := sstable.NewReader(newSSTPath)
			if err != nil {
				// ... (错误处理) ...
				lsm.flushDone <- struct{}{}
				continue
			}

			// 4. 将新 Reader 添加到 L0
			lsm.lock.Lock()
			lsm.levelManager.AddL0(reader)
			lsm.immutableMemtable = nil
			lsm.lock.Unlock()

			// 5. !! 核心：清理旧的 WAL !!
			// 这是数据安全的最后一步。
			// 只有SSTable成功写入并注册后，才能删除对应的WAL。
			if task.wal != nil {
				if err := task.wal.Clear(); err != nil {
					// 只是无法清理，问题不大，记录日志
					fmt.Printf("WARN: failed to clear old wal %s: %v\n",
						task.wal.file.Name(), err)
				}
			}

			// 6. 通知 Put 操作刷写已完成
			lsm.flushDone <- struct{}{}
		}
	}
}

// Put 写入一个键值对
// (Delete 只是 isTombstone 为 true 的 Put)
func (lsm *LSM) Put(key, value []byte, isTombstone bool) error {
	// 1. 写 WAL
	if err := lsm.wal.Write(key, value, isTombstone); err != nil {
		return fmt.Errorf("failed to write WAL: %w", err)
	}

	lsm.lock.Lock()
	lsm.activeMemtable.Put(key, value, isTombstone)

	// 3. 检查大小
	if lsm.activeMemtable.ApproximateSize() < lsm.memtableSizeLimit {
		lsm.lock.Unlock()
		return nil
	}

	// 4. Memtable 已满，触发刷写 (Flush)

	// (a) 等待上一次刷写完成
	if lsm.immutableMemtable != nil {
		lsm.lock.Unlock()
		<-lsm.flushDone
		lsm.lock.Lock()
	}

	// (b) 将 active 变为 immutable
	lsm.immutableMemtable = lsm.activeMemtable
	lsm.activeMemtable = NewSkipList(defaultMaxLevel)

	// (c) !! 核心：轮转 (Rotate) WAL !!
	oldWalPath := lsm.wal.file.Name()
	// 用时间戳重命名
	ts := time.Now().UnixNano()
	newWalSegmentPath := filepath.Join(
		lsm.dirPath,
		fmt.Sprintf("wal_%d.log", ts),
	)

	// 关闭当前的 WAL
	if err := lsm.wal.Close(); err != nil {
		lsm.lock.Unlock()
		return fmt.Errorf("failed to close active WAL: %w", err)
	}

	// 重命名
	if err := os.Rename(oldWalPath, newWalSegmentPath); err != nil {
		lsm.lock.Unlock()
		return fmt.Errorf("failed to rename WAL: %w", err)
	}

	// 打开一个新的 active WAL
	newWal, err := NewWAL(oldWalPath) // 还是用 "wal.log" 这个名字
	if err != nil {
		lsm.lock.Unlock()
		return fmt.Errorf("failed to create new WAL: %w", err)
	}
	lsm.wal = newWal // lsm.wal 现在指向新的空文件

	// 打开一个指向*旧* WAL (wal_...log) 的实例，用于清理
	oldWal, err := NewWAL(newWalSegmentPath)
	if err != nil {
		// 这个问题不大，只是无法清理，但数据是安全的
		fmt.Printf("WARN: failed to open old WAL for clearing: %v\n", err)
	}

	// (d) 创建刷写任务
	task := flushTask{
		mem: lsm.immutableMemtable,
		wal: oldWal,
	}

	lsm.lock.Unlock()

	// (e) 发送任务
	lsm.flushChan <- task

	return nil
}

// Delete 是 Put 的封装
func (lsm *LSM) Delete(key []byte) error {
	return lsm.Put(key, nil, true)
}

// Get 查找一个键
// 返回 (value, found)
func (lsm *LSM) Get(key []byte) ([]byte, bool, error) {
	lsm.lock.RLock()
	defer lsm.lock.RUnlock()

	// 查 Active Memtable (最新)
	val, tomb, found := lsm.activeMemtable.Get(key)
	if found {
		if tomb {
			return nil, false, nil // 已被删除
		}
		return val, true, nil
	}

	// 查 Immutable Memtable (次新)
	if lsm.immutableMemtable != nil {
		val, tomb, found = lsm.immutableMemtable.Get(key)
		if found {
			if tomb {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	// 查 L0 SSTables (从新到旧)
	// l0Readers 是一个快照
	l0Readers := lsm.levelManager.GetL0Readers()

	lsm.lock.RUnlock() // Get 的主要锁在这里释放
	// 我们在 L0 列表的末尾追加新文件，所以倒序遍历
	for i := len(l0Readers) - 1; i >= 0; i-- {
		reader := l0Readers[i]
		val, tomb, found, err := reader.Get(key)
		if err != nil {
			return nil, false, fmt.Errorf("error reading sstable %d: %w", i, err)
		}

		if found {
			if tomb {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	// (TODO) 查询 L1 ... LN (目前 L1+ 为空)

	// 未找到
	return nil, false, nil
}
