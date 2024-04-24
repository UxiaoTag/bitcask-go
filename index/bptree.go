package index

import (
	"bitcask-go/data"
	"path/filepath"

	"go.etcd.io/bbolt"
)

//B+树索引，然后将索引持久化到磁盘啊
//封装使用了 go.etcd.io/bbolt

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(path string, syncWrite bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrite
	bptree, err := bbolt.Open(filepath.Join(path, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}
	//创建相关的bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{
		tree: bptree,
	}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldVal = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value (in bucket) in bptree")
	}
	if len(oldVal) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldVal)
}
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		println(err.Error())
		panic("failed to get value (in bucket) in bptree")
	}
	return pos
}
func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldVal = bucket.Get(key); len(oldVal) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value (in bucket) in bptree")
	}
	if len(oldVal) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldVal), true
}

// 返回创建的索引迭代器
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

// 返回大小
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to Size() (in bucket) in bptree")
	}
	return size
}

// 关闭
func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// B+Tree迭代器
type bptreeIterator struct {
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor
	reverse bool
	key     []byte
	value   []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	//手动开启一个事务
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

// 回到迭代器起点
func (bpti *bptreeIterator) Rewind() {
	if !bpti.reverse {
		bpti.key, bpti.value = bpti.cursor.First()
	} else {
		bpti.key, bpti.value = bpti.cursor.Last()
	}
}

// 根据传入key值找到第一个大于或小于等于目标的key，根据这个key开始bianli
func (bpti *bptreeIterator) Seek(key []byte) {
	bpti.key, bpti.value = bpti.cursor.Seek(key)
}

// 下一个key
func (bpti *bptreeIterator) Next() {
	if !bpti.reverse {
		bpti.key, bpti.value = bpti.cursor.Next()
	} else {
		bpti.key, bpti.value = bpti.cursor.Prev()
	}
}

// 是否有效，指key是否遍历完毕
func (bpti *bptreeIterator) Valid() bool {
	return len(bpti.value) != 0
}

// 遍历当前位置Key
func (bpti *bptreeIterator) Key() []byte {
	return bpti.key
}

// 遍历当前位置Value，这里指内存找到的下标
func (bpti *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpti.value)
}

// 关闭迭代器
func (bpti *bptreeIterator) Close() {
	//只读事务必须使用RollBack而不是commit
	bpti.tx.Rollback()
}
