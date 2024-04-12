package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Iterator //索引迭代器
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(Options IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(Options.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   Options,
	}
}

// 回到迭代器起点
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// 根据传入key值找到第一个大于或小于等于目标的key，根据这个key开始bianli
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// 下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// 是否有效，指key是否遍历完毕
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// 遍历当前位置Key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// 遍历当前位置Value，这里指数据文件
func (it *Iterator) Value() ([]byte, error) {
	pos := it.indexIter.Value()

	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(pos)

}

// 关闭迭代器
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	preFixlen := len(it.options.Prefix)
	//没设定就不用过滤
	if preFixlen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if preFixlen <= len(key) && bytes.Equal(it.options.Prefix, it.indexIter.Key()[:preFixlen]) {
			break
		}
	}

}
