package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
)

// ART树
// 封装于https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// 初始化art树
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	art.tree.Insert(key, pos)
	return true
}
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}
func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	_, deleted := art.tree.Delete(key)
	return deleted
}

// 返回创建的索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art.tree == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}

// 返回大小
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return art.tree.Size()
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// art 索引迭代器,这里因为art本身的迭代器不支持相关的操作
// 所以只能牺牲内存容量，去做一个Item数组，用来做迭代操作
type artIterator struct {
	currIndex int     //当前遍历的下标
	reverse   bool    //是否是反向遍历
	values    []*Item //索引内的信息
}

func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int

	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValue := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValue)
	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// 回到迭代器起点
func (ait *artIterator) Rewind() {
	ait.currIndex = 0
}

// 根据传入key值找到第一个大于(或小于)等于目标的key，根据这个key开始遍历
func (ait *artIterator) Seek(key []byte) {
	//二分查找
	if ait.reverse {
		ait.currIndex = sort.Search(len(ait.values), func(i int) bool {
			return bytes.Compare(ait.values[i].key, key) <= 0
		})
	} else {
		ait.currIndex = sort.Search(len(ait.values), func(i int) bool {
			return bytes.Compare(ait.values[i].key, key) >= 0
		})
	}
}

// 下一个key
func (ait *artIterator) Next() {
	ait.currIndex++
}

// 是否有效，如果表示true则表示currIndex还在下标内，false则代表currIndex无效了
func (ait *artIterator) Valid() bool {
	return ait.currIndex < len(ait.values)
}

// 遍历当前位置Key
func (ait *artIterator) Key() []byte {
	return ait.values[ait.currIndex].key
}

// 遍历当前位置Value，这里数据文件拿取数据
func (ait *artIterator) Value() *data.LogRecordPos {
	return ait.values[ait.currIndex].pos
}

// 关闭迭代器
func (ait *artIterator) Close() {
	//这个最大，清了就完了
	ait.values = nil
}
