package index

import (
	"bitcask-go/data"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPTPut(t *testing.T) {
	path, _ := os.Getwd()
	path = filepath.Join(path, "tmp")
	tree := NewBPlusTree(path, false)

	oldVal := tree.Put([]byte("aas"), &data.LogRecordPos{1, 4, 4})
	assert.Nil(t, oldVal)
	oldVal = tree.Put([]byte("a24s"), &data.LogRecordPos{784, 4321, 4})
	assert.Nil(t, oldVal)

	oldVal = tree.Put([]byte("dawsdas"), &data.LogRecordPos{24, 35443, 4})
	assert.Nil(t, oldVal)

	oldVal = tree.Put([]byte("dawsdas"), &data.LogRecordPos{11, 22, 4})
	assert.Equal(t, oldVal.Fid, uint32(24))
	assert.Equal(t, oldVal.Offset, int64(35443))
	tree.tree.Close()
	os.RemoveAll(path)
}

func TestBPTGet(t *testing.T) {
	path, _ := os.Getwd()
	path = filepath.Join(path, "tmp")
	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("no key"))
	t.Log(pos)
	assert.Nil(t, pos)
	tree.Put([]byte("aas"), &data.LogRecordPos{1, 4, 4})
	pos = tree.Get([]byte("aas"))
	t.Log(pos)

	tree.Put([]byte("aas"), &data.LogRecordPos{224, 21234, 4})
	pos = tree.Get([]byte("aas"))
	t.Log(pos)

	// t.Fail()
}

func TestBPTDelete(t *testing.T) {
	path, _ := os.Getwd()
	path = filepath.Join(path, "tmp")
	tree := NewBPlusTree(path, false)

	i, isEmpty := tree.Delete([]byte("no key"))
	t.Log(i)
	assert.Nil(t, i)
	assert.False(t, isEmpty)

	tree.Put([]byte("aas"), &data.LogRecordPos{224, 21234, 4})

	pos := tree.Get([]byte("aas"))
	t.Log(pos)
	assert.NotNil(t, pos)

	i, isEmpty = tree.Delete([]byte("aas"))
	t.Log(i)
	assert.Equal(t, i.Fid, uint32(224))
	assert.Equal(t, i.Offset, int64(21234))
	assert.True(t, isEmpty)

	pos = tree.Get([]byte("aas"))
	assert.Nil(t, pos)
	t.Log(pos)

	// t.Fail()
}

func TestBPTSize(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-BPTSize")
	os.MkdirAll(dir, os.ModePerm)
	defer os.RemoveAll(dir)
	tree := NewBPlusTree(dir, false)
	t.Log(tree.Size())
	assert.Equal(t, tree.Size(), 0)
	tree.Put([]byte("aas"), &data.LogRecordPos{1, 4, 4})
	tree.Put([]byte("a24s"), &data.LogRecordPos{784, 432, 41})
	tree.Put([]byte("dawsdas"), &data.LogRecordPos{24, 354, 443})

	t.Log(tree.Size())

	assert.Equal(t, tree.Size(), 3)
	// t.Fail()
}

func TestBPT_IT(t *testing.T) {
	dir, _ := os.MkdirTemp("", "BPT-IT")
	os.MkdirAll(dir, os.ModePerm)
	defer os.RemoveAll(dir)
	tree := NewBPlusTree(dir, false)
	tree.Put([]byte("aas"), &data.LogRecordPos{1, 4, 4})
	tree.Put([]byte("a24s"), &data.LogRecordPos{784, 432, 41})
	tree.Put([]byte("dawsdas"), &data.LogRecordPos{24, 3, 45443})
	tree.Put([]byte("56ws23s"), &data.LogRecordPos{24, 354, 443})
	tree.Put([]byte("89sadwds23s"), &data.LogRecordPos{24, 3544, 43})
	tree.Put([]byte("s9s2423s"), &data.LogRecordPos{24, 3544, 43})

	bpit := tree.Iterator(false)

	for bpit.Rewind(); bpit.Valid(); bpit.Next() {
		t.Log(string(bpit.Key()))
	}
	// t.Fail()

}
