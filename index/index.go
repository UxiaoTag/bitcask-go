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
	//返回创建的索引迭代器
	Iterator(reverse bool) Iterator
	//返回大小
	Size() int
	//关闭索引
	Close() error
}

type IndexType = int8

// 这里枚举,索引类型
const (
	//BTree索引
	Btree IndexType = iota + 1
	//ART自适应树
	ART
	//B+Tree,且持久化到磁盘
	BPTree
)

// 根据索引类型初始化索引
func NewIndexer(typ IndexType, dirPath string, syncWrite bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, syncWrite)
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

// 通用索引迭代器
type Iterator interface {
	//回到迭代器起点
	Rewind()

	//根据传入key值找到第一个大于或小于等于目标的key，根据这个key开始bianli
	Seek(key []byte)

	//下一个key
	Next()

	//是否有效，指key是否遍历完毕
	Valid() bool

	//遍历当前位置Key
	Key() []byte

	//遍历当前位置Value，这里指内存找到的下标
	Value() *data.LogRecordPos

	//关闭迭代器
	Close()
}
