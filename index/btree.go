package index

import (
	"bitcask-go/data"
	"sync"

	"github.com/google/btree"
)

//BTree索引，这个接口的作用封装google的btree作为数据结构,实现index.go

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

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(it)

	return true
}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btItem := bt.tree.Get(it)
	if btItem == nil {
		return nil
	}
	return btItem.(*Item).pos
}
func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	btItem := bt.tree.Delete(it)
	return btItem != nil
}
