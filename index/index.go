package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// Indexer 抽象索引接口，后续如果想要接入其他数据结构，实现对应接口即可
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

type IndexType = int8

// 这里枚举,索引类型
const (
	//BTree索引
	Btree IndexType = iota + 1
	//ART自适应树
	ART
)

// 根据索引类型初始化索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		//todo
		return nil
	default:
		panic("unkown IndexType")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// 必须实现，需要有一个弱序的比较规则，用于判断树的存放
func (ia *Item) Less(ib btree.Item) bool {
	return bytes.Compare(ia.key, ib.(*Item).key) == -1
}

func (it *Item) Key() interface{} {
	return it.key
}
