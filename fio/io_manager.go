package fio

import "io/fs"

const DatafilePerm fs.FileMode = 0644

// 抽象IO管理接口，可以接入不同的IO类型，目前先用标准文件的IO
type IOManager interface {
	// 从文件的给定位置读取对应数据
	Read([]byte, int64) (int, error)
	// 写入字节到文件中
	Write([]byte) (int, error)
	// Sync持久化数据
	Sync() error
	// Close关闭IO
	Close() error
	//Size获取文件大小
	Size() (int64, error)
}

//初始化IOManager,目前只支持标准FileIO
func NewIOManager(filename string) (IOManager, error) {
	return NewFileIOManager(filename)
}
