//go:build windows
// +build windows

package bitcask_go

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

type Stat struct {
	KeyNum          uint  //key的数量
	DataFileNum     uint  //数据文件的数量
	ReclaimableSize int64 //磁盘可回收字节空间，单位为字节
	DiskSize        int64 //所占磁盘空间
}

// 获取一个目录的占用大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// 获取磁盘可用的空间
func AvailableDiskSize() (uint64, error) {
	//用这个来做
	h := windows.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")

	// 获取当前执行程序的路径，提取其驱动器部分
	exePath, err := os.Executable()
	if err != nil {
		return 0, err
	}
	drive := exePath[:3] // 例如 "C:\program.exe" 取 "C:"
	// 将字符串转换为 syscall.UTF16Ptr 格式，因为 GetDiskFreeSpaceEx 需要这样的格式
	drivePtr, err := syscall.UTF16PtrFromString(drive)
	if err != nil {
		return 0, err
	}
	lpFreeBytesAvailable := uint64(0)
	lpTotalNumberOfBytes := uint64(0)
	lpTotalNumberOfFreeBytes := uint64(0)
	//这里windows api使用call返回1.函数执行成功与否的状态码。2.某些函数会有第二个属于函数本身的返回值，3.错误
	ret, _, err := c.Call(uintptr(unsafe.Pointer(drivePtr)),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)))
	if ret == 0 {
		return 0, err
	}
	return lpTotalNumberOfFreeBytes, nil
}

