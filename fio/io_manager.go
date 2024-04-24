package fio

import "io/fs"

const DatafilePerm fs.FileMode = 0644

//IO类型
type FileIOType = byte

const (
	//标准文件IO
	StandardFIO FileIOType = iota

	//MemoryMap内存文件映射
	MemroyMap
)

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
func NewIOManager(filename string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(filename)
	case MemroyMap:
		return NewMMapIOManager(filename)
	default:
		panic("Unknow IOType!")
	}
}
