package bitcask_go

import "os"

type Options struct {
	DirPath string //数据库数据路径

	DataFileSize int64 //配置数据文件大小

	SyncWrites bool //是否每次都写入文件都进行持久化

	BytesPerSync uint //积累到多少字节后进行持久化

	MmapAtStartup bool //启动是否使用mmap加载

	IndexType IndexerType //索引类型

	DataFileMergeRatio float32 //数据合并的阈值
}

type IteratorOptions struct {
	//遍历指定前缀为指定值，默认为空
	Prefix []byte
	//是否为反向遍历,false为正向
	Reverse bool
}

type WriteBatchOptions struct {
	//一批次最大的数据量
	MaxBatchNum uint
	//每次提交事务都进行持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	//Btree索引
	Btree IndexerType = iota + 1

	//ART自适应树索引
	ART

	//B+Tree主要持久化索引
	BPTree
)

var DefaultDBOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024, //256M
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          ART,
	MmapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultIterOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
