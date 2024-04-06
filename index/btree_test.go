package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTreePut(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)
	res1 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 7})
	res2 := bt.Put([]byte("b"), &data.LogRecordPos{Fid: 3, Offset: 2})
	res3 := bt.Put([]byte("y"), &data.LogRecordPos{Fid: 4, Offset: 235})
	assert.True(t, res1)
	assert.True(t, res2)
	assert.True(t, res3)
}

func TestBTreeGet(t *testing.T) {

	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)
	res1 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 7})
	res2 := bt.Put([]byte("b"), &data.LogRecordPos{Fid: 3, Offset: 2})
	res3 := bt.Put([]byte("y"), &data.LogRecordPos{Fid: 4, Offset: 235})
	res4 := bt.Put([]byte("y"), &data.LogRecordPos{Fid: 7, Offset: 2235})
	assert.True(t, res1)
	assert.True(t, res2)
	assert.True(t, res3)

	pos := bt.Get(nil)
	assert.Equal(t, uint32(1), pos.Fid)
	assert.Equal(t, int64(100), pos.Offset)

	pos1 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(7), pos1.Offset)

	pos2 := bt.Get([]byte("b"))
	assert.Equal(t, uint32(3), pos2.Fid)
	assert.Equal(t, int64(2), pos2.Offset)

	assert.True(t, res4)

	pos3 := bt.Get([]byte("y"))
	assert.Equal(t, uint32(7), pos3.Fid)
	assert.Equal(t, int64(2235), pos3.Offset)

}

func TestBTreeDelete(t *testing.T) {

	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)
	res1 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 7})
	res2 := bt.Put([]byte("b"), &data.LogRecordPos{Fid: 3, Offset: 2})
	res3 := bt.Put([]byte("y"), &data.LogRecordPos{Fid: 4, Offset: 235})

	assert.True(t, res1)
	assert.True(t, res2)
	assert.True(t, res3)

	ok := bt.Delete(nil)

	assert.True(t, ok)

	ok = bt.Delete([]byte("b"))

	assert.True(t, ok)

	ok = bt.Delete([]byte("aaa"))

	assert.False(t, ok)

}
