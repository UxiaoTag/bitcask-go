package bitcask_go

import (
	"bitcask-go/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const mergeDirName = "-merge"

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

	//查看是否达到阈值
	size, err := DirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if float32(db.reclaimSize)/float32(size) < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}

	//查看剩余空间是否足够merge
	freeSize, err := AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(size-db.reclaimSize) >= freeSize {
		db.mu.Unlock()
		return ErrNoFreeSpaceForMerge
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
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
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

	//这里需要逐一close，尝试过会报错
	if err := mergedb.Close(); err != nil {
		return err
	}
	if err := hintFile.Close(); err != nil {
		return err
	}
	if err := MergeFinishedFile.Close(); err != nil {
		return err
	}

	return nil
}

// eg /tmp/bitcask /tmp/bitcask-merge
func (db *DB) getMergePath() string {
	dirPath := db.options.DirPath
	//windows使用os.MkdirTemp("","file-id")，会出现文件dir和base函数压根无法使用的问题。
	dir := path.Dir(path.Clean(dirPath))
	if dir == "." { //说明无法识别
		dirPath = strings.ReplaceAll(dirPath, "\\", "/")
		dir = path.Dir(path.Clean(dirPath))
	}
	//获取路径的最后一个文件
	base := path.Base(dirPath)
	return filepath.Join(dir, base+mergeDirName)
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
		//临时数据库关闭后会触发保存SeqNoFileName，这个是无效文件，甚至会影响原来的SeqNo，需要删掉
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		//flock文件也不需要粘贴过去
		if entry.Name() == fileLockName {
			continue
		}
		//如果是bptree-index也不应该传过去，首先BPTree是持久化的索引，他就已经记录好了最新的索引。直接替代会丢失所有数据,merge过程没有记录任何索引，
		//实测rename还会报错，因为你再上面已经创建了index，所以这里bptree会被占用导致失败。这里跳过
		if entry.Name() == bptreeIndexName {
			continue
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

// 这里找到MergeFile然后读取fileId
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
		//这里是只考虑bptree的情况
		if db.options.IndexType == BPTree {
			nonMergeFileId, err := db.getNonMergeFileId(db.options.DirPath)
			if err != nil {
				return err
			}
			oldPos := db.index.Get(logRecord.Key)
			//当你的oldPos的取出来(不是被delete掉了)，且取出来的值小于你merge的值，我觉得应该是
			//这里说明如果hint取出来一个key在index找不到，默认index在接下来的过程中删掉了该key
			//如果能取出来且得出的fileid是以前的，merge之前的文件，默认没改过，修正pos
			//感觉这里的默认还是有点绝对，还需要做更细致的判断
			if oldPos != nil && oldPos.Fid < nonMergeFileId {
				db.index.Put(logRecord.Key, pos)
			}
		} else {
			db.index.Put(logRecord.Key, pos)
		}
		offset += size
	}

	return nil
}
