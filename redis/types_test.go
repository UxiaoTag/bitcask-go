package redis

import (
	bitcask_go "bitcask-go"
	"bitcask-go/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(10))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(10))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	t.Log(string(val1))

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)
	t.Log(string(val2))

	_, err = rds.Get(utils.GetTestKey(3))
	assert.Equal(t, err, bitcask_go.ErrKeyNotFound)

	// t.Fail()
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-del-type")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Del(utils.GetTestKey(2))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(31), 0, utils.RandomValue(10))
	assert.Nil(t, err)

	//type
	typ, err := rds.Type(utils.GetTestKey(31))
	assert.Nil(t, err)
	t.Log(typ)
	assert.Equal(t, typ, String)

	err = rds.Del(utils.GetTestKey(31))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(31))
	t.Log(err)
	assert.Equal(t, err, bitcask_go.ErrKeyNotFound)

	// t.Fail()
}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	t.Log(ok1, err)
	assert.True(t, ok1)

	v := utils.RandomValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v)
	t.Log(string(v))
	t.Log(ok2, err)
	assert.False(t, ok2)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	t.Log(ok3, err)
	assert.True(t, ok1)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	t.Log(string(val1), err)
	assert.Equal(t, val1, v)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	t.Log(string(val2), err)
	assert.Equal(t, val2, v2)

	val3, err := rds.HGet(utils.GetTestKey(1), []byte("field-notfound"))
	t.Log(string(val3), err)

	// t.Fail()
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hdel")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.HDel(utils.GetTestKey(123), []byte("field1"))
	assert.False(t, ok)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	t.Log(ok1, err)
	assert.True(t, ok1)
	assert.Nil(t, err)

	v := utils.RandomValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v)
	t.Log(string(v))
	t.Log(ok2, err)
	assert.False(t, ok2)
	assert.Nil(t, err)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	t.Log(ok3, err)
	assert.True(t, ok1)
	assert.Nil(t, err)

	ok4, err := rds.HDel(utils.GetTestKey(1), []byte("field2"))
	t.Log(ok4, err)
	assert.Nil(t, err)
	assert.True(t, ok4)

	// t.Fail()

}

func TestRedisDataStructure_HKeys(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hkeys")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	t.Log(ok1, err)
	assert.True(t, ok1)
	assert.Nil(t, err)

	v := utils.RandomValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v)
	t.Log(string(v))
	t.Log(ok2, err)
	assert.False(t, ok2)
	assert.Nil(t, err)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	t.Log(ok3, err)
	assert.True(t, ok1)
	assert.Nil(t, err)

	fields, err := rds.Hkeys(utils.GetTestKey(1))
	assert.Nil(t, err)

	for _, field := range fields {
		t.Log(string(field))
	}

	// t.Fail()
}

func TestRedisDataStructure_HVals(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hvals")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	t.Log(ok1, err)
	assert.True(t, ok1)
	assert.Nil(t, err)

	v := utils.RandomValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v)
	t.Log(string(v))
	t.Log(ok2, err)
	assert.False(t, ok2)
	assert.Nil(t, err)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	t.Log(ok3, err)
	t.Log(string(v2))
	assert.True(t, ok1)
	assert.Nil(t, err)

	values, err := rds.Hvals(utils.GetTestKey(1))
	assert.Nil(t, err)

	for _, value := range values {
		t.Log(string(value))
	}

	t.Fail()
}

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-sismember")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-srem")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_LPop(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-lpop")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	res, err := rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestRedisDataStructure_RPop(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-rpop")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	res, err := rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestRedisDataStructure_ZScore(t *testing.T) {
	opts := bitcask_go.DefaultDBOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-zset")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.ZAdd(utils.GetTestKey(1), 113, []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 333, []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 98, []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	score, err := rds.ZScore(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, float64(333), score)
	score, err = rds.ZScore(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, float64(98), score)
}
