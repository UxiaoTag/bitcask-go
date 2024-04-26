package redis

import (
	"bitcask-go/utils"
	"encoding/binary"
	"math"
)

const (
	//mete数据最大值
	maxMetadataSize       = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetadataSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte   //数据类型
	expire   int64  //过期时间
	version  int64  //版本号
	size     uint32 //数据量
	head     uint64 //use for List
	tail     uint64 //use for List
}

func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetadataSize
	}
	buf := make([]byte, size)

	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]

	var index = 1
	expire, i := binary.Varint(buf[index:])
	index += i
	version, i := binary.Varint(buf[index:])
	index += i
	size, i := binary.Varint(buf[index:])
	index += i
	var head, tail uint64 = 0, 0
	if dataType == List {

		head, i = binary.Uvarint(buf[index:])
		index += i
		tail, _ = binary.Uvarint(buf[index:])
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)

	//key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	if hk.field != nil {
		//field
		copy(buf[index:], hk.field)
	}
	return buf
}

func hashInternalKeyDecode(buf []byte, keySize int) *hashInternalKey {
	var index = 0
	key := make([]byte, keySize)
	//取出key
	copy(buf[index:index+keySize], key)
	index += keySize

	//version
	version := binary.LittleEndian.Uint64(buf[index : index+8])
	index += 8

	//field buf[index:]全是field

	return &hashInternalKey{
		key:     key,
		version: int64(version),
		field:   buf[index:],
	}
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8)

	//key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	//member,这里不添加membersize因为，membersize在尾行，直接读取全部就好了
	if sk.member != nil {
		//member
		copy(buf[index:], sk.member)
	}

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8*2)

	//key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	//index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member []byte
	score   float64
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+len(zk.member)+8)

	//key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	//member
	copy(buf[index:], zk.member)
	index += len(zk.member)

	return buf
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+8) //+4

	//key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	//score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	//member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	//member size
	// binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf

}
