package bitcask_go

type Options struct {
	DirPath string //数据库数据路径

	DataFileSize int64 //配置数据文件大小

	SyncWrites bool //是否每次都写入文件都进行持久化

	IndexType IndexerType //索引类型
}

type IndexerType = int8

const (
	//Btree索引
	Btree IndexerType = iota + 1

	//ART自适应树索引
	ART
)
