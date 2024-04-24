package bitcask_go

import (
	"bitcask-go/utils"
	"bytes"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		//目前测试下来所有结果都报错RemoveAll失败，但是测试都通过了,可能是windows的问题
		//panic: remove C:\Users\xiao\AppData\Local\Temp\bitcask-go-delete138827274\000000000.data:
		//The process cannot access the file because it is being used by another process. [recovered]
		//经过测试发现直接使用原作者的代码也会出现这个问题，可能跟windows的权限有关，不太想研究
		//找到原因了db2,未close的问题
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			fmt.Println("remove not!!!")
			// panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	db.Close()
}

func TestDB_Put(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-put")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.写到数据文件进行了转换
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.oldFiles))

	// 6.重启后再 Put 数据
	db.Close() // todo 实现 Close 方法后这里用 Close() 替代
	// err = db.activeFile.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
	db2.Close()
}

func TestDB_Get(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-get")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常读取一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 3.值被重复 Put 后在读取
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4.值被删除后再 Get
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	// assert.Equal(t, 2, len(db.oldFiles))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// 6.重启后，前面写入的数据都能拿到
	err = db.Close() // todo 实现 Close 方法后这里用 Close() 替代
	// err = db.activeFile.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	val6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	//db1我前面关了，后面出问题了
	val8, err := db2.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
	db2.Close()
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close() // todo 实现 Close 方法后这里用 Close() 替代
	// err = db.activeFile.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
	db2.Close()
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-ListKey")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))

	//add one
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	keys = db.ListKeys()
	t.Log(len(keys))

	//add many
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(43), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(34), utils.RandomValue(24))
	assert.Nil(t, err)
	keys = db.ListKeys()
	t.Log(len(keys))
	for _, key := range keys {
		t.Log(string(key))
	}
	// t.Fail()

}

func TestDB_Fold(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-Fold")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//add many
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(43), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(34), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(74), utils.RandomValue(24))
	assert.Nil(t, err)

	db.Fold(func(key, value []byte) bool {
		t.Log(string(key))
		t.Log(string(value))
		return !bytes.Equal(key, utils.GetTestKey(34))
	})
	// t.Fail()
}

func TestDB_Close(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-Close")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//add 1
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(24))
	assert.Nil(t, err)

}

func TestDB_Sync(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-Sync")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//add 1
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(24))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)

}

func TestDB_fileLock(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-FileLock")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	db2, err := Open(opts)
	t.Log(err)
	t.Log(db2)
	assert.Equal(t, ErrDatabaseIsUsing, err)
	assert.Nil(t, db2)

	err = db.Close()
	assert.Nil(t, err)

	db2, err = Open(opts)
	t.Log(err)
	t.Log(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	// t.Fail()
}

// this test use for mmapTestTime test
func TestDB_Write1000000(t *testing.T) {
	opts := DefaultDBOptions
	dir, err := os.Getwd()
	assert.Nil(t, err)
	dir = path.Join(dir, "tmp", "bitcask-go-mmapTest")
	opts.DirPath = dir
	// opts.IndexType = ART
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	for i := 0; i < 3000000; i++ {
		db.Put(utils.GetTestKey(i), utils.RandomValue(10))
	}
	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_MMapTime(t *testing.T) {
	opts := DefaultDBOptions
	dir, err := os.Getwd()
	assert.Nil(t, err)
	dir = path.Join(dir, "tmp", "bitcask-go-mmapTest")
	opts.DirPath = dir
	// opts.IndexType = ART
	opts.DataFileSize = 64 * 1024 * 1024
	opts.MmapAtStartup = true
	now := time.Now()
	db, err := Open(opts)

	t.Log("open Time", time.Since(now))

	assert.Nil(t, err)
	assert.NotNil(t, db)
	// t.Fail()
}

func TestDB_Stat(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-Stat")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 100; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	for i := 100; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 2000; i < 5000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	stat := db.Stat()

	t.Log(stat.DataFileNum)
	t.Log(stat.DiskSize)
	t.Log(stat.KeyNum)
	t.Log(stat.ReclaimableSize)
	t.Fail()
}
