package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

// BTree索引，这个接口的作用封装google的btree作为数据结构,实现index.go
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree初始化BTree索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldIt := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldIt == nil {
		return nil
	}
	return oldIt.(*Item).pos
}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btItem := bt.tree.Get(it)
	if btItem == nil {
		return nil
	}
	return btItem.(*Item).pos
}
func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	btItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if btItem == nil {
		return nil, false
	}
	return btItem.(*Item).pos, true
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	return nil
}

// BTree 索引迭代器,这里因为btree本身的迭代器不支持相关的操作
// 所以只能牺牲内存容量，去做一个Item数组，用来做迭代操作
type btreeIterator struct {
	currIndex int     //当前遍历的下标
	reverse   bool    //是否是反向遍历
	values    []*Item //索引内的信息
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	saveValue := func(it btree.Item) bool {
		//btree.Item是一个interface，所以这里的意思是，将传入的it尝试转换为实现了btree.Item接口的函数，比如Item就是。
		//如果失败会出现panic
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValue)
	} else {
		tree.Ascend(saveValue)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// 回到迭代器起点
func (bit *btreeIterator) Rewind() {
	bit.currIndex = 0
}

// 根据传入key值找到第一个大于(或小于)等于目标的key，根据这个key开始遍历
func (bit *btreeIterator) Seek(key []byte) {
	//低效的逐步遍历
	// if bit.reverse {
	// 	for id, item := range bit.values {
	// 		//这里的意思当前key<=传入key,
	// 		if bytes.Compare(item.key, key) <= 0 {
	// 			bit.currIndex = id
	// 		}
	// 	}
	// } else {
	// 	for id, item := range bit.values {
	// 		//这里的意思当前key>=传入key,
	// 		if bytes.Compare(item.key, key) >= 0 {
	// 			bit.currIndex = id
	// 		}
	// 	}
	// }
	//二分查找
	if bit.reverse {
		bit.currIndex = sort.Search(len(bit.values), func(i int) bool {
			return bytes.Compare(bit.values[i].key, key) <= 0
		})
	} else {
		bit.currIndex = sort.Search(len(bit.values), func(i int) bool {
			return bytes.Compare(bit.values[i].key, key) >= 0
		})
	}
}

// 下一个key
func (bit *btreeIterator) Next() {
	bit.currIndex++
}

// 是否有效，如果表示true则表示currIndex还在下标内，false则代表currIndex无效了
func (bit *btreeIterator) Valid() bool {
	return bit.currIndex < len(bit.values)
}

// 遍历当前位置Key
func (bit *btreeIterator) Key() []byte {
	return bit.values[bit.currIndex].key
}

// 遍历当前位置Value，这里数据文件拿取数据
func (bit *btreeIterator) Value() *data.LogRecordPos {
	return bit.values[bit.currIndex].pos
}

// 关闭迭代器
func (bit *btreeIterator) Close() {
	//这个最大，清了就完了
	bit.values = nil
}
