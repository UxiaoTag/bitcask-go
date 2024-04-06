package data

//索引的数据结构，主要描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 //文件id，表示数据存放到文件的哪个位置
	Offset int64  //表示偏移量
}
