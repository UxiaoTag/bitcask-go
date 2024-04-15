package bitcask_go

import (
	"bitcask-go/utils"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-WriteBatch-1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//no commit
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	value, err := db.Get(utils.GetTestKey(1))
	t.Log(value, err)
	//sould be nil
	assert.Equal(t, err, ErrKeyNotFound)

	//commit put
	err = wb.Commit()
	assert.Nil(t, err)

	value1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, value1)
	assert.Nil(t, err)
	t.Log(value1, err)

	//delete commit
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	value2, err := db.Get(utils.GetTestKey(1))
	t.Log(value2, err)
	assert.Equal(t, err, ErrKeyNotFound)

	// t.Fail()
}

func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-WriteBatch-2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.GetTestKey(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	wb.Put(utils.GetTestKey(5), utils.RandomValue(10))
	wb.Put(utils.GetTestKey(22), utils.RandomValue(10))
	wb.Put(utils.GetTestKey(6), utils.RandomValue(10))

	err = wb.Commit()
	assert.Nil(t, err)

	//重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	val, err := db2.Get(utils.GetTestKey(1))
	t.Log(val)
	t.Log(err)
	assert.Equal(t, err, ErrKeyNotFound)

	assert.Equal(t, uint64(2), 2)
	db2.Close()
	// t.Fail()
}

func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultDBOptions
	// dir, _ := os.MkdirTemp("", "bitcask-go-WriteBatch-3")
	dir, err := os.Getwd()
	assert.Nil(t, err)
	dir = path.Join(dir, "tmp", "bitcask-go-WriteBatch-3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	// defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	// for i := 0; i < 5000; i++ {
	// 	err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
	// 	assert.Nil(t, err)
	// }
	// err = wb.Commit()
	// assert.Nil(t, err)
	value, err := db.Get(utils.GetTestKey(11))
	t.Log(value, err)

	keys := db.ListKeys()
	t.Log(len(keys))
	// err = db.Close()
	// assert.Nil(t, err)
	t.Fail()
}
