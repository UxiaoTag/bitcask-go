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

	tree.Put([]byte("aas"), &data.LogRecordPos{1, 4})
	tree.Put([]byte("a24s"), &data.LogRecordPos{784, 4321})
	tree.Put([]byte("dawsdas"), &data.LogRecordPos{24, 35443})
}

func TestBPTGet(t *testing.T) {
	path, _ := os.Getwd()
	path = filepath.Join(path, "tmp")
	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("no key"))
	t.Log(pos)
	assert.Nil(t, pos)
	tree.Put([]byte("aas"), &data.LogRecordPos{1, 4})
	pos = tree.Get([]byte("aas"))
	t.Log(pos)

	tree.Put([]byte("aas"), &data.LogRecordPos{224, 21234})
	pos = tree.Get([]byte("aas"))
	t.Log(pos)

	// t.Fail()
}

func TestBPTDelete(t *testing.T) {
	path, _ := os.Getwd()
	path = filepath.Join(path, "tmp")
	tree := NewBPlusTree(path, false)

	i := tree.Delete([]byte("no key"))
	t.Log(i)

	tree.Put([]byte("aas"), &data.LogRecordPos{224, 21234})

	pos := tree.Get([]byte("aas"))
	t.Log(pos)
	assert.NotNil(t, pos)

	i = tree.Delete([]byte("aas"))
	t.Log(i)

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
	tree.Put([]byte("aas"), &data.LogRecordPos{1, 4})
	tree.Put([]byte("a24s"), &data.LogRecordPos{784, 4321})
	tree.Put([]byte("dawsdas"), &data.LogRecordPos{24, 35443})

	t.Log(tree.Size())

	assert.Equal(t, tree.Size(), 3)
	// t.Fail()
}

func TestBPT_IT(t *testing.T) {
	dir, _ := os.MkdirTemp("", "BPT-IT")
	os.MkdirAll(dir, os.ModePerm)
	defer os.RemoveAll(dir)
	tree := NewBPlusTree(dir, false)
	tree.Put([]byte("aas"), &data.LogRecordPos{1, 4})
	tree.Put([]byte("a24s"), &data.LogRecordPos{784, 4321})
	tree.Put([]byte("dawsdas"), &data.LogRecordPos{24, 35443})
	tree.Put([]byte("56ws23s"), &data.LogRecordPos{24, 35443})
	tree.Put([]byte("89sadwds23s"), &data.LogRecordPos{24, 35443})
	tree.Put([]byte("s9s2423s"), &data.LogRecordPos{24, 35443})

	bpit := tree.Iterator(false)

	for bpit.Rewind(); bpit.Valid(); bpit.Next() {
		t.Log(string(bpit.Key()))
	}
	// t.Fail()

}
