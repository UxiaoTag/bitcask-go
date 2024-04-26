package redis

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
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

// ----------------------------------------------------Set数据结构-----------------------------------------------------------------------------

func (rds *ReadisDataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok = false
	if _, err := rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		meta.size++
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		wb.Put(key, meta.encode())
		wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

func (rds *ReadisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}

	return true, nil
}

func (rds *ReadisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	//数据引擎错误
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}

	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}

	//更新
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	wb.Put(key, meta.encode())
	wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ----------------------------------------------------List数据结构-----------------------------------------------------------------------------

func (rds *ReadisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *ReadisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *ReadisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *ReadisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *ReadisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	//构造key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	//把meta的代码写在这里就没必要判断俩次了
	if isLeft {
		//这里最尾部的设计是meta.tail=为空 遍历为[head,tail-1]
		lk.index = meta.head - 1
		meta.head--
	} else {
		lk.index = meta.tail
		meta.tail++
	}

	//更新meta和key
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size++
	wb.Put(key, meta.encode())
	wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil

}

func (rds *ReadisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	//把meta的代码写在这里就没必要判断俩次了
	if isLeft {
		lk.index = meta.head
		meta.head++
	} else {
		//这里最尾部的设计是meta.tail=为空 遍历为[head,tail-1]
		lk.index = meta.tail - 1
		meta.tail--
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}
	meta.size--

	//TODO做个wb把取出的element值也删掉
	err = rds.db.Put(key, meta.encode())
	if err != nil {
		return nil, err
	}

	return element, nil
}

//TODO List做个根据index读取和修改 element的方法

// ----------------------------------------------------ZSet数据结构-----------------------------------------------------------------------------

func (rds *ReadisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	//构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
		score:   score,
	}

	//查看是否存在
	var exits = true
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		exits = false
	}

	if exits {
		//score和member都一致，没有必要改
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	//更新数据和元数据
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	if !exits {
		meta.size++
		wb.Put(key, meta.encode())
	}
	if exits {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		wb.Delete(oldKey.encodeWithScore())
	}

	wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(zk.score))
	wb.Put(zk.encodeWithScore(), nil)

	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exits, nil
}

func (rds *ReadisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	//构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}
