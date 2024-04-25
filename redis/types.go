package redis

import (
	bitcask "bitcask-go"
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// readis数据结构服务
type ReadisDataStructure struct {
	db *bitcask.DB
}

// 初始化
func NewRedisDataStructure(Options bitcask.Options) (*ReadisDataStructure, error) {
	db, err := bitcask.Open(Options)
	if err != nil {
		return nil, err
	}
	return &ReadisDataStructure{
		db: db,
	}, err
}

// ----------------------------------------------------String数据结构-----------------------------------------------------------------------------
// set放置string类型数据
func (rds *ReadisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	//编码value: type+expire+payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	index := 1
	var expire int64 = 0
	if ttl != 0 {
		//过期时间,纳秒
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	//调用DB自带的PUT存储
	return rds.db.Put(key, encValue)
}

func (rds *ReadisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	//解码
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, i := binary.Varint(encValue[index:])
	index += i

	//判断时间
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}

// ----------------------------------------------------Hash数据结构-----------------------------------------------------------------------------

// HashSet方法,此处bool返回为该field是否存在
func (rds *ReadisDataStructure) HSet(key, field, value []byte) (bool, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	//构造hash key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	//先查找对应的key是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	//不存在更新元数据
	if !exist {
		meta.size++
		wb.Put(key, meta.encode())
	}

	_ = wb.Put(encKey, value)
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *ReadisDataStructure) HGet(key, field []byte) ([]byte, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	//构造hash key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	return rds.db.Get(hk.encode())
}

func (rds *ReadisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	//构造hash key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	var exist = true
	if _, err := rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}
	if exist {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

func (rds *ReadisDataStructure) Hkeys(key []byte) ([][]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	//构造hash key，这里留白field
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		// field:   field,
	}

	//定义该key的前缀
	encKey := hk.encode()

	//设定前缀key
	itOps := bitcask.DefaultIterOptions
	itOps.Prefix = encKey

	it := rds.db.NewIterator(itOps)

	var fields [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		hk := hashInternalKeyDecode(it.Key(), len(key))
		if hk.field != nil {
			//转存
			fields = append(fields, hk.field)
		}
	}

	it.Close()

	return fields, nil
}

func (rds *ReadisDataStructure) Hvals(key []byte) ([][]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	//构造hash key，这里留白field
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		// field:   field,
	}

	//定义该key的前缀
	encKey := hk.encode()

	//设定前缀key
	itOps := bitcask.DefaultIterOptions
	itOps.Prefix = encKey

	it := rds.db.NewIterator(itOps)

	var values [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			return nil, err
		}
		if value != nil {
			//转存
			values = append(values, value)
		}
	}

	it.Close()

	return values, nil
}
