package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ph0m1/kv/storage/sstable"
)

const (
	// Memtable 达到 64MB 时刷写
	defaultMemtableSizeLimit = 64 * 1024 * 1024
	// WAL 文件名
	walFileName = "wal.log"

	targetSSTableSize = 64 * 1024 * 1024
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
	flushChan         chan flushTask // 通知 flushWorker 工作的通道
	flushDone         chan struct{}  // flushWorker 完成时的信号
	stopChan          chan struct{}  // 关闭引擎的信号
	compactionTrigger chan struct{}  // 压缩触发信号

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
		compactionTrigger: make(chan struct{}),
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
	go lsm.compactorWorker() // 启动压缩器

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

			//  将新 Reader 添加到 L0
			lsm.lock.Lock()
			lsm.levelManager.AddL0(reader)
			lsm.immutableMemtable = nil
			lsm.lock.Unlock()

			//  !! 核心：清理旧的 WAL !!
			// 这是数据安全的最后一步。
			// 只有SSTable成功写入并注册后，才能删除对应的WAL。
			if task.wal != nil {
				if err := task.wal.Clear(); err != nil {
					// 只是无法清理，问题不大，记录日志
					fmt.Printf("WARN: failed to clear old wal %s: %v\n",
						task.wal.file.Name(), err)
				}
			}

			//  通知 Put 操作刷写已完成
			lsm.flushDone <- struct{}{}

			// 触发压缩检查
			lsm.triggerCompaction()
		}
	}
}

// runCompaction 执行一次压缩任务
// 这是 LSM-Tree 的核心归并排序算法
func (lsm *LSM) runCompaction(task *CompactionTask) ([]*sstable.Reader, error) {
	// 1. 为所有输入文件创建迭代器
	iters := make([]sstable.Iterator, len(task.InputFiles))
	for i, reader := range task.InputFiles {
		iter, err := reader.NewIterator()
		if err != nil {
			// ... (错误处理: 关闭所有已打开的迭代器) ...
			return nil, fmt.Errorf("failed to create iterator: %w", err)
		}
		// *sstable.sstableIterator 隐式满足 sstable.Iterator 接口
		iters[i] = iter
	}

	// defer 确保所有迭代器都被关闭
	defer func() {
		for _, iter := range iters {
			if iter != nil { // 确保 iter 不是 nil 接口
				iter.Close()
			}
		}
	}()

	// 2. 创建并初始化最小堆
	h := newCompactionHeap(iters)
	if h.Len() == 0 {
		return make([]*sstable.Reader, 0), nil // 没有输入，无需压缩
	}

	// 3. 准备输出 (SSTable Writer)
	var newReaders []*sstable.Reader
	// var lastKeyWritten []byte

	newSSTNum := lsm.levelManager.NextFileNumber()
	newSSTPath := filepath.Join(lsm.dirPath, fmt.Sprintf("%04d.sst", newSSTNum))
	outputWriter, err := sstable.NewWriter(newSSTPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output writer: %w", err)
	}

	// 4. 核心归并循环
	for {
		// 4a. 弹出堆顶 (这是全局最小键，且是*最新*版本)
		item, ok := h.PopAndRefill()
		if !ok {
			break // 堆已空，压缩完成
		}

		// 4b. !! 关键去重逻辑 !!
		// h.Less() 保证了我们 Pop 出来的是*最新*版本。
		// 我们现在需要丢弃所有*旧*版本 (即堆中所有同key的条目)。
		for {
			next, ok := h.Peek()
			if !ok {
				break // 堆空了
			}

			// 如果下一个键和当前键不同，停止去重
			if bytes.Compare(next.key, item.key) != 0 {
				break
			}

			// 键相同，说明是旧版本，弹出并丢弃
			h.PopAndRefill()
		}

		// 4c. !! 关键墓碑逻辑 !!
		// 经过 4b，我们手上拿的是 `item.key` 的唯一、最新的版本。
		// 如果这个最新版本是“墓碑”，我们就*不*把它写入新文件。
		// 这就同时删除了 "墓碑" 和所有它覆盖的旧数据。
		if item.isTombstone {
			continue // 丢弃
		}

		// 4d. !! 关键：输出切分 (Split Output) !!
		// 如果当前 SSTable 写入过大，则关闭它，创建新的
		if outputWriter.CurrentSize() > targetSSTableSize {
			if err := outputWriter.Close(); err != nil {
				return nil, err
			}
			// 为刚写完的文件创建 Reader
			reader, err := sstable.NewReader(newSSTPath)
			if err != nil {
				return nil, err
			}
			newReaders = append(newReaders, reader)

			// 创建一个新的 Writer
			newSSTNum = lsm.levelManager.NextFileNumber()
			newSSTPath = filepath.Join(lsm.dirPath, fmt.Sprintf("%04d.sst", newSSTNum))
			outputWriter, err = sstable.NewWriter(newSSTPath)
			if err != nil {
				return nil, err
			}
		}

		// 4e. 写入数据
		outputWriter.Write(item.key, item.value, item.isTombstone)
		// lastKeyWritten = item.key
	}

	// 关闭最后一个 Writer
	if outputWriter.CurrentSize() > 0 {
		if err := outputWriter.Close(); err != nil {
			return nil, err
		}
		reader, err := sstable.NewReader(newSSTPath)
		if err != nil {
			return nil, err
		}
		newReaders = append(newReaders, reader)
	}

	return newReaders, nil
}

// compactorWorker (压缩器的主循环)
func (lsm *LSM) compactorWorker() {
	for {
		select {
		case <-lsm.stopChan:
			return
		case <-lsm.compactionTrigger:
			// 开始检查压缩任务
			// 使用 for 循环处理 级联压缩
			// 只要 levelManager 能找到任务就一直做下去
			for {
				task := lsm.levelManager.FindCompactionTask()
				if task == nil {
					break
				}

				// 执行压缩（runCompaction(task)）
				newReaders, err := lsm.runCompaction(task)
				if err != nil {
					fmt.Printf("ERROR: failed to run compaction: %v\n", err)
					// RETRY
					break
				}

				// 原子地提交结果
				obsoleteReaders, err := lsm.levelManager.ApplyCompactionResult(task, newReaders)
				if err != nil {
					fmt.Printf("ERROR: failed to apply compaction: %v\n", err)
					// TODO: 需要关闭newReader 并删除文件
					fmt.Println("ROLLBACK: cleaning up newly create orphan files...")
					for _, r := range newReaders {
						r.Close()
						if errRem := os.Remove(r.FilePath()); errRem != nil {
							fmt.Printf("ERROR: failed to remove orphan file %s: %v\n",
								r.FilePath(), errRem)
						}
					}
					break
				}

				// 物理清除
				for _, reader := range obsoleteReaders {
					reader.Close()
					if err := os.Remove(reader.FilePath()); err != nil {
						// 记录错误，但不停止
						fmt.Printf("ERROR: failed to remove obsolete sstable %s: %v\n", reader.FilePath(), err)
					}
				}
				// 压缩完成，立即再次检查是否有级联任务
			}
		}
	}
}

// 触发压缩的辅助函数 (非阻塞)
func (lsm *LSM) triggerCompaction() {
	select {
	case lsm.compactionTrigger <- struct{}{}:
		// 信号发送成功
	default:
		// 如果触发器已经有一个信号了 (compactor 还没来得及处理)
		// 那就没必要再发了，跳过即可
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

	// Memtable 已满，触发刷写 (Flush)

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

	// 查 Active Memtable (最新)
	val, tomb, found := lsm.activeMemtable.Get(key)

	// 查 Immutable Memtable (次新)
	if !found && lsm.immutableMemtable != nil {
		val, tomb, found = lsm.immutableMemtable.Get(key)
	}

	// 获取所有SSTable的“快照”
	// 我们在释放锁 *之前* 获取快照，以保证
	// Memtable 和 SSTable 之间的一致性。
	snapshot := lsm.levelManager.GetSnapshot()

	lsm.lock.RUnlock()

	defer snapshot.Close()

	// 接下来只操作快照

	if found {
		if tomb {
			return nil, false, nil // 已被删除
		}
		return val, true, nil
	}

	// 查 L0 SSTables (从新到旧)
	// l0Readers 是一个快照
	l0Readers := snapshot.levels[0]

	// 我们在 L0 列表的末尾追加新文件，所以倒序遍历
	for i := len(l0Readers) - 1; i >= 0; i-- {
		reader := l0Readers[i]
		val, tomb, found, err := reader.Get(key)
		if err != nil {
			return nil, false, fmt.Errorf("error reading L0 sstable %d: %w", i, err)
		}

		if found {
			if tomb {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	for level := 1; level < maxLevels; level++ {
		readers := snapshot.levels[level]

		if len(readers) == 0 {
			continue // 本层空
		}

		// 在 L1+ 中查找
		// L1+ 的 SSTable 按 minKey 顺序， 且互补重叠
		// 二分
		i := sort.Search(len(readers), func(i int) bool {
			return bytes.Compare(readers[i].MaxKey(), key) >= 0
		})

		// 检查是否找到了
		if i >= len(readers) {
			// key 比本层所有文件的 maxKey 都大
			continue
		}

		// 找到了第 i 个文件 且 i.maxKey >= key
		// 检查 MinKey <= key
		// 如果 MinKey > key，说明 key 掉在缝隙里，本层不存在
		reader := readers[i]
		if bytes.Compare(reader.MinKey(), key) <= 0 {
			// 找到了唯一的目标文件
			val, tomb, found, err := reader.Get(key)
			if err != nil {
				return nil, false, fmt.Errorf("error reading L%d sstable: %W", i, err)
			}
			if found {
				if tomb {
					return nil, false, nil // 已被删除
				}
				return val, true, nil
			}
			// (如果在这个文件中没找到，说明它在LSM-Tree中不存在，
			//  因为 L1+ 不重叠，它也不可能在更低层)
			// (但它可能在下一层，所以不立即返回)
		}
	}

	// 未找到
	return nil, false, nil
}
