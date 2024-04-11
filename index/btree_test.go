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

func TestBtree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	//空btree
	it1 := bt1.Iterator(false)
	assert.Equal(t, false, it1.Valid())

	//add 1
	bt1.Put([]byte("aaac"), &data.LogRecordPos{1, 2})
	it2 := bt1.Iterator(false)
	assert.Equal(t, true, it2.Valid())
	t.Log(it2.Key())
	t.Log(it2.Value())
	it2.Next()
	assert.Equal(t, false, it2.Valid())

	//add n
	bt1.Put([]byte("aaac2"), &data.LogRecordPos{1, 42})
	bt1.Put([]byte("aa2ac"), &data.LogRecordPos{21, 2})
	bt1.Put([]byte("a2aac"), &data.LogRecordPos{134, 2234})
	bt1.Put([]byte("2aaac"), &data.LogRecordPos{7, 262})
	bt1.Put([]byte("aavv"), &data.LogRecordPos{21, 2})
	bt1.Put([]byte("bbaac"), &data.LogRecordPos{134, 2234})
	bt1.Put([]byte("ee2ac"), &data.LogRecordPos{21, 2})
	bt1.Put([]byte("qqaac"), &data.LogRecordPos{134, 2234})

	it3 := bt1.Iterator(false)
	for it3.Rewind(); it3.Valid(); it3.Next() {
		t.Log("key=", string(it3.Key()), "value", it3.Value())
	}

	//close and seek
	it4 := bt1.Iterator(true)
	for it4.Seek([]byte("aa")); it4.Valid(); it4.Next() {
		t.Log("key=", string(it4.Key()), "value", it4.Value())
	}

	// it4.Seek([]byte("z"))
	// t.Log(string(it4.Key()))

	it4.Close()
	it4.Rewind()
	assert.Equal(t, false, it4.Valid())

	t.Fail()

}
