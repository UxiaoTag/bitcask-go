package bitcask_go

import (
	"bitcask-go/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const MergeBase = "-merge"

// TODO
// Merge清理数据
func (db *DB) Merge() error {
	//db为空
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	//标识正在merging
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}
	//开始merging
	db.isMerging = true
	//结束设回false
	defer func() {
		db.isMerging = false
	}()

	//处理当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	//当前活跃文件纳入oldfiles
	db.oldFiles[db.activeFile.FileId] = db.activeFile
	//设置新活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	//记录没merge的文件
	nonMergeFileId := db.activeFile.FileId

	//取出需要merge的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.oldFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	//给数据文件排序，从小到大merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	//如果存在，说明之前调用过，就删了
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	//新建merger目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	//打开一个临时的bitcask实例
	mergeOption := db.options
	mergeOption.DirPath = mergePath
	//因为如果因为零时关闭导致merge失败，这些数据我们是不需要的。不如自己控制sync
	mergeOption.SyncWrites = false
	mergedb, err := Open(mergeOption)
	if err != nil {
		return err
	}

	//打开hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	//遍历处理每个数据文件
	for _, datafile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := datafile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//解析拿到的key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			//比较内存索引和当前位置的区别，如果与索引一致，就重写到临时目录
			if logRecordPos != nil && logRecordPos.Fid == datafile.FileId && logRecordPos.Offset == offset {
				//这时候放进去其实不用关系事务的id,故直接用无事务id
				//为什么不使用更上层的方法
				// mergedb.Put(logRecordKeyAddSeq(realKey, nonTransactionSeqNo), logRecord.Value)
				logRecord.Key = logRecordKeyAddSeq(realKey, nonTransactionSeqNo)
				pos, err := mergedb.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				//当前索引写到Hint文件
				if err := hintFile.WriteHintRecord(logRecord.Key, pos); err != nil {
					return err
				}
			}
			//下一条
			offset += size
		}
	}

	//sync保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergedb.Sync(); err != nil {
		return err
	}

	// 写标识的merge完成的文件
	MergeFinishedFile, err := data.OpenMergeFinishFile(mergePath)
	if err != nil {
		return nil
	}
	mergeFinishRecord := &data.LogRecord{
		Key:   []byte("mergeFinishedKey"),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinishRecord)
	if err := MergeFinishedFile.Write(encRecord); err != nil {
		return err
	}

	//持久化标识的merge完成的文件
	if err := MergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// eg /tmp/bitcask /tmp/bitcask-merge
func (db *DB) getMergePath() string {
	dirPath := db.options.DirPath
	dir := path.Dir(path.Clean(dirPath))
	//获取路径的最后一个文件
	base := path.Base(dirPath)
	return filepath.Join(dir, base+MergeBase)
}

// 加载merge数据目录
func (db *DB) loadMergeFile() error {
	mergePath := db.getMergePath()

	//如果目录不存在没必要进行
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	//查找merge的完成标识符号
	var mergeFinished bool
	var mergeFileName []string
	for _, entry := range dirEntries {
		//说明完成了
		if entry.Name() == data.MergeFinishedName {
			mergeFinished = true
		}
		mergeFileName = append(mergeFileName, entry.Name())
	}

	//merge未完成
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	//删除旧数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		filename := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(filename); err == nil {
			if err := os.Remove(filename); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动过来
	for _, filename := range mergeFileName {
		// /temp/bitcask-marge 000.data 001.data
		// update to  /temp/bitcask 000.data 001.data
		srcPath := filepath.Join(mergePath, filename)
		destPath := filepath.Join(db.options.DirPath, filename)
		err := os.Rename(srcPath, destPath)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) getNonMergeFileId(mergePath string) (uint32, error) {
	hintFinishFile, err := data.OpenMergeFinishFile(mergePath)
	if err != nil {
		return 0, err
	}
	finishRecord, _, err := hintFinishFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(finishRecord.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

func (db *DB) loadIndexFromHintFile() error {
	//查看hint文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//打开hint索引文件
	hintfile, err := data.OpenHintFile(db.options.DirPath)

	if err != nil {
		return err
	}

	//读取hint索引文件
	var offset int64 = 0
	for {
		logRecord, size, err := hintfile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}

	return nil
}
