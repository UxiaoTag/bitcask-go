package bitcask_go

import "os"

type Options struct {
	DirPath string //数据库数据路径

	DataFileSize int64 //配置数据文件大小

	SyncWrites bool //是否每次都写入文件都进行持久化

	IndexType IndexerType //索引类型
}

type IteratorOptions struct {
	//遍历指定前缀为指定值，默认为空
	Prefix []byte
	//是否为反向遍历,false为正向
	Reverse bool
}

type IndexerType = int8

const (
	//Btree索引
	Btree IndexerType = iota + 1

	//ART自适应树索引
	ART
)

var DefaultDBOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, //256M
	SyncWrites:   false,
	IndexType:    Btree,
}

var DefaultIterOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
