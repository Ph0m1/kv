package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ph0m1/kv/storage/sstable"
)

const (
	maxLevels           = 7
	l0CompactionTrigger = 4 // 当 L0 的文件达到 4 个时，触发 L0 -> L1 压缩

	// L1 的目标大小 (例如 10MB)
	baseLevelSize = 10 * 1024 * 1024
	// L_N+1 是 L_N 的 10 倍
	levelSizeMultiplier = 10
)

// 一个压缩任务
type CompactionTask struct {
	Level       int // 输入层
	OutputLevel int // 输出层

	InputFiles []*sstable.Reader // 需要被合并的 SSTable
}

// LevelManager 负责跟踪所有层级的 SSTable
type LevelManager struct {
	lock sync.RWMutex

	// levels[0] 是 L0
	// levels[1...N] 是 L1...
	levels [][]*sstable.Reader

	dirPath string

	// 文件序号生成器，取代 lsm.go 中的 buggy 逻辑
	nextFileNum int
}

// levelSnapshot 提供了 Get() 操作所需的所有SSTable的
// 一个一致的、时间点(point-in-time)的视图
type levelSnapshot struct {
	levels [][]*sstable.Reader
}

// Close 释放快照持有的所有 Reader
func (s *levelSnapshot) Close() {
	// do nothing
}

// GetSnapshot 获取所有层级的原子快照
func (lm *LevelManager) GetSnapshot() *levelSnapshot {
	lm.lock.RLock() // <-- 获取读锁
	defer lm.lock.RUnlock()

	snap := &levelSnapshot{
		levels: make([][]*sstable.Reader, maxLevels),
	}

	for level, readers := range lm.levels {
		// 必须返回一个 *副本*，以防止 Compactor 修改
		// 正在被 Get() 使用的切片。
		snap.levels[level] = make([]*sstable.Reader, len(readers))
		copy(snap.levels[level], readers)
	}

	return snap
}

// NewLevelManager 创建或加载层级管理器
func NewLevelManager(dirPath string) (*LevelManager, error) {
	lm := &LevelManager{
		levels:      make([][]*sstable.Reader, maxLevels),
		dirPath:     dirPath,
		nextFileNum: 1, // 总是从 1 开始
	}

	for i := 0; i < maxLevels; i++ {
		lm.levels[i] = make([]*sstable.Reader, 0)
	}

	// 加载所有现有的 .sst 文件
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read data dir: %w", err)
	}

	var sstFiles []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".sst") {
			sstFiles = append(sstFiles, f.Name())
		}
	}
	sort.Strings(sstFiles) // 确保顺序

	for _, fileName := range sstFiles {
		sstPath := filepath.Join(dirPath, fileName)
		reader, err := sstable.NewReader(sstPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load sstable %s: %w", fileName, err)
		}

		// 重点：我们如何知道它属于哪一层？
		// 缺乏 "MANIFEST" 文件，我们只能做一个*简化*：
		// 暂时将所有文件都加载到 L0
		// (这是一个临时措施，压缩器稍后会修复它)
		lm.levels[0] = append(lm.levels[0], reader)

		// 更新文件序号
		num, _ := strconv.Atoi(strings.TrimSuffix(fileName, ".sst"))
		if num >= lm.nextFileNum {
			lm.nextFileNum = num + 1
		}
	}
	return lm, nil
}

// AddL0 将一个新刷写的 SSTable 添加到 L0
func (lm *LevelManager) AddL0(reader *sstable.Reader) {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	// L0 的新文件总是在末尾 (最新的)
	lm.levels[0] = append(lm.levels[0], reader)
}

// NextFileNumber 返回一个唯一的 SSTable 文件编号
func (lm *LevelManager) NextFileNumber() int {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	num := lm.nextFileNum
	lm.nextFileNum += 1
	return num
}

// GetReadView 返回一个用于读取的“快照”
// 这是一个关键的并发控制点
// (目前实现比较简单，只是返回 L0)
func (lm *LevelManager) GetL0Readers() []*sstable.Reader {
	lm.lock.RLock()
	defer lm.lock.RUnlock()

	// 必须复制一份切片，防止在 Get 查询时
	// Compactor 修改了底层的 l0 列表
	readers := make([]*sstable.Reader, len(lm.levels[0]))
	copy(readers, lm.levels[0])
	return readers
}

func (lm *LevelManager) FindCompactionTask() *CompactionTask {
	lm.lock.RLock()
	defer lm.lock.RUnlock()

	// 策略 1: L0 -> L1 压缩
	// 这是最高优先级的压缩
	if len(lm.levels[0]) >= l0CompactionTrigger {
		// 选择 L0 的所有文件
		l0Files := make([]*sstable.Reader, len(lm.levels[0]))
		copy(l0Files, lm.levels[0])

		// 找出 L0 文件的总键范围
		var minKey, maxKey []byte
		for _, r := range l0Files {
			if minKey == nil || bytes.Compare(r.MinKey(), minKey) < 0 {
				minKey = r.MinKey()
			}
			if maxKey == nil || bytes.Compare(r.MaxKey(), maxKey) > 0 {
				maxKey = r.MaxKey()
			}
		}

		// 选择 L1 中与 L0 总范围重叠的文件
		l1Files := lm.findOverlappingFiles(1, minKey, maxKey)

		// 为了让 'compactor_heap' 正确工作 (最新版本优先)，
		// L0 的文件 (索引更大) 必须在 L1 文件之后。
		inputFiles := make([]*sstable.Reader, 0, len(l0Files)+len(l1Files))
		inputFiles = append(inputFiles, l1Files...)
		inputFiles = append(inputFiles, l0Files...)

		return &CompactionTask{
			Level:       0,
			OutputLevel: 1,
			InputFiles:  l0Files,
		}
	}

	// 策略 2: L_N -> L_N+1 压缩 (级联压缩)
	for level := 1; level < maxLevels-1; level++ {
		// 如果没满，则检查下一层
		if lm.levelTotalSize(level) < lm.levelTargetSize(level) {
			continue
		}

		// 本层已满，需要压缩到下一层

		// 只选择 L_N 中的 *一个* 文件来“推送”
		// (策略：选择最老的，即列表中的第一个)
		// (L1+ 列表是按 MinKey 排序的)
		fileToCompact := lm.levels[level][0]

		// 找到 L_N+1 中与这个文件重叠的所有文件
		lNp1Files := lm.findOverlappingFiles(
			level+1,
			fileToCompact.MinKey(),
			fileToCompact.MaxKey(),
		)

		// 同样，L_N 的文件 (fileToCompact) 必须在 L_N+1 文件之后。
		inputFiles := make([]*sstable.Reader, 0, 1+len(lNp1Files))
		inputFiles = append(inputFiles, lNp1Files...)
		inputFiles = append(inputFiles, fileToCompact)

		return &CompactionTask{
			Level:       level,
			OutputLevel: level + 1,
			InputFiles:  inputFiles,
		}

	}

	// 没有任务
	return nil
}

// findOverlappingFiles 查找指定层级中，与 [minKey, maxKey] 范围重叠的文件
func (lm *LevelManager) findOverlappingFiles(level int, minKey, maxKey []byte) []*sstable.Reader {
	if minKey == nil && maxKey == nil {
		return nil // L0 为空，无需重叠
	}

	overlapping := make([]*sstable.Reader, 0)

	// L1+ 层的文件是按 MinKey 排序的，我们可以高效查找
	for _, r := range lm.levels[level] {
		// "不重叠" 的条件:
		// 1. 文件的 MaxKey 在 minKey 之前
		// 2. 文件的 MinKey 在 maxKey 之后

		noOverlap := (bytes.Compare(r.MaxKey(), minKey) < 0) ||
			(bytes.Compare(r.MinKey(), maxKey) > 0)

		if !noOverlap {
			overlapping = append(overlapping, r)
		}
	}
	return overlapping
}

// 临时函数 (仅供 Phase A 测试)
// 在 Phase B 中, ApplyCompactionResult 将会取代这个
func (lm *LevelManager) RemoveTempCompactionFiles(task *CompactionTask) {
	if task.Level == 0 {
		lm.lock.Lock()
		// 简单地清空 L0
		// 警告: 这是一个不安全的操作，仅用于防止死循环
		lm.levels[0] = make([]*sstable.Reader, 0)
		lm.lock.Unlock()
	}
}

func (lm *LevelManager) ApplyCompactionResult(task *CompactionTask, newReaders []*sstable.Reader) ([]*sstable.Reader, error) {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	// --- 验证 ---
	// 确保 task.InputFiles 仍然存在与层级中，防止重复应用

	// --- 移除旧文件 (InputFiles) ---
	// 我们需要一个辅助函数来从 slice 中移除元素

	// 从 L0 移除
	if task.Level == 0 {
		// L0 的输入文件总是 *所有* L0 文件
		lm.levels[0] = make([]*sstable.Reader, 0)
	} else {
		// L_N (N > 0)
		lm.levels[task.Level] = removeReaders(lm.levels[task.Level], task.InputFiles)
	}

	// (如果 L0->L1 压缩还涉及 L1 的文件，也需要从 L1 移除)
	// (我们目前的 FindCompactionTask 简化了这一点，所以暂时跳过)

	// --- 3. 添加新文件 (newReaders) ---
	lm.levels[task.OutputLevel] = append(lm.levels[task.OutputLevel], newReaders...)

	// --- 4. (重要！) 保持 L1+ 层的有序性 ---
	// L1+ 层的文件必须按 Key 排序，以便 Get() 可以二分查找
	if task.OutputLevel > 0 {
		sort.Slice(lm.levels[task.OutputLevel], func(i, j int) bool {
			// 按照 minKey 升序排序
			return bytes.Compare(
				lm.levels[task.OutputLevel][i].MinKey(),
				lm.levels[task.OutputLevel][j].MinKey(),
			) < 0
		})
	}

	// --- 5. 返回需要被物理删除的旧文件 ---
	return task.InputFiles, nil
}

// (在文件末尾) 添加一个辅助函数：
// removeReaders 从 'a' 中移除 'b' 中的所有元素
func removeReaders(a []*sstable.Reader, b []*sstable.Reader) []*sstable.Reader {
	// 创建一个 map 用于快速查找 'b' 中的元素
	removeSet := make(map[*sstable.Reader]bool)
	for _, r := range b {
		removeSet[r] = true
	}

	// 创建一个新的 slice，只包含不在 'b' 中的元素
	newSlice := make([]*sstable.Reader, 0, len(a))
	for _, r := range a {
		if !removeSet[r] {
			newSlice = append(newSlice, r)
		}
	}
	return newSlice
}

// levelTargetSize 返回一个层级的目标总大小
func (lm *LevelManager) levelTargetSize(level int) int64 {
	if level == 0 {
		// L0 的大小是动态的，由 l0CompactionTrigger (文件数) 控制
		return 0
	}
	size := int64(baseLevelSize)
	for i := 1; i < level; i++ {
		size *= levelSizeMultiplier
	}
	return size
}

// levelTotalSize 返回一个层级中所有文件的总大小
func (lm *LevelManager) levelTotalSize(level int) int64 {
	var total int64
	// 被 FindCompactionTask 调用且已持有锁
	for _, reader := range lm.levels[level] {
		total += reader.FileSize()
	}
	return total
}
