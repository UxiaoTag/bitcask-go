package bitcask_go

import (
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iteration")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	it := db.NewIterator(DefaultIterOptions)
	assert.NotNil(t, db)
	assert.Equal(t, false, it.Valid())
	// t.Fail()
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iteration")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	it := db.NewIterator(DefaultIterOptions)
	assert.NotNil(t, db)
	assert.Equal(t, true, it.Valid())
	assert.Equal(t, utils.GetTestKey(10), it.Key())
	t.Log(string(it.Key()))
	val, err := it.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), val)
	t.Log(string(val))
	// t.Fail()
}

func TestDB_Iterator_Many_Value(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iteration-2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	db.Put([]byte("aabc"), utils.RandomValue(10))
	db.Put([]byte("ccbd"), utils.RandomValue(10))
	db.Put([]byte("eeqc"), utils.RandomValue(10))
	db.Put([]byte("xxwq"), utils.RandomValue(10))

	it := db.NewIterator(DefaultIterOptions)
	for it.Rewind(); it.Valid(); it.Next() {
		value, err := it.Value()
		assert.Nil(t, err)
		println("key:", string(it.Key()), " value:", string(value))
	}

	println("---------------------------------------------------------------------")
	//反向迭代
	op := DefaultIterOptions
	op.Reverse = true
	it2 := db.NewIterator(op)
	for it2.Rewind(); it2.Valid(); it2.Next() {
		value, err := it2.Value()
		assert.Nil(t, err)
		println("key:", string(it2.Key()), " value:", string(value))
	}

	println("---------------------------------------------------------------------")

	//过滤迭代
	for it.Seek([]byte("d")); it.Valid(); it.Next() {
		value, err := it.Value()
		assert.Nil(t, err)
		println("key:", string(it.Key()), " value:", string(value))
	}

	println("---------------------------------------------------------------------")

	//过滤迭代2
	for it2.Seek([]byte("d")); it2.Valid(); it2.Next() {
		value, err := it2.Value()
		assert.Nil(t, err)
		println("key:", string(it2.Key()), " value:", string(value))
	}

	println("---------------------------------------------------------------------")

	//指定PreFix
	op.Reverse = false
	op.Prefix = []byte("aa")
	it3 := db.NewIterator(op)
	for it3.Rewind(); it3.Valid(); it3.Next() {
		value, err := it3.Value()
		assert.Nil(t, err)
		println("key:", string(it3.Key()), " value:", string(value))
	}

	t.Fail()
}
