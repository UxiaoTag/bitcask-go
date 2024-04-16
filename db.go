// 面向用户的操作接口
package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// bitcask存储引擎实例
type DB struct {
	mu         *sync.RWMutex
	options    Options
	fileIds    []int                     //use for load index
	index      index.Indexer             //内存索引
	activeFile *data.DataFile            //当前活跃文件，用于写入
	oldFiles   map[uint32]*data.DataFile //旧数据文件，只用于读
	seqNo      uint64                    //事务执行的序列号
	isMerging  bool
}

// 打开bitcask数据库引擎
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	//判断数据库目录是存在，如果不存在的话，就创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		//创建目录
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	db := &DB{
		mu:       new(sync.RWMutex),
		options:  options,
		oldFiles: make(map[uint32]*data.DataFile),
		index:    index.NewIndexer(options.IndexType),
	}

	//加载merge数据目录
	if err := db.loadMergeFile(); err != nil {
		return nil, err
	}

	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//从hint索引中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	//数据文件加载内存索引
	if err := db.loadIndexFromDatafile(); err != nil {
		return nil, err
	}

	return db, nil
}

// 写入Key/Value数据，key不能为空
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//构造LogRecord
	log := &data.LogRecord{
		Key:   logRecordKeyAddSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//追加写入当前活跃数据文件中
	pos, err := db.appendLogRecordWithLock(log)
	if err != nil {
		return err
	}

	//结束后更新内存中的索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFail
	}
	return nil
}

// 通过Key获取value数据，key不能为空
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//从index读取索引信息
	pos := db.index.Get(key)
	//这里处理key不存在
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	//从数据文件中取出value
	return db.getValueByPosition(pos)
}

// 根据索引从数据获取对应value
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	//根据fid找到对应数据文件
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[pos.Fid]
	}

	//找不到数据文件
	if dataFile == nil {
		return nil, ErrNoDataFile
	}

	//根据偏移读取数据
	LogRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	//如果删之后，不太能理解，这里应该get不到LogRecordDelete的情况
	if LogRecord.Type == data.LogRecordDelete {
		return nil, err
	}

	return LogRecord.Value, err
}

// 写入Key/Value数据，key不能为空
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//如果你读取的key不存在或已经删除，就没必要再追加写入当前活跃数据文件中
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	//构造LogRecord
	log := &data.LogRecord{
		Key:  logRecordKeyAddSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDelete,
	}

	//追加写入当前活跃数据文件中
	_, err := db.appendLogRecordWithLock(log)
	if err != nil {
		return err
	}

	//结束后更新内存中的索引
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFail
	}
	return nil
}

// 从数据库中获取所有的key
func (db *DB) ListKeys() [][]byte {
	it := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	idx := 0
	for it.Rewind(); it.Valid(); it.Next() {
		keys[idx] = it.Key()
		idx++
	}
	return keys
}

// 获取所有数据，并执行用户指定操作,当函数return false终止循环
func (db *DB) Fold(fun func(key []byte, value []byte) bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	it := db.index.Iterator(false)
	for it.Rewind(); it.Valid(); it.Next() {
		//取出value
		value, err := db.getValueByPosition(it.Value())
		if err != nil {
			return err
		}
		//对key,value执行fun函数
		//如果用户返回false，就停止循环
		if !fun(it.Key(), value) {
			break
		}
	}
	return nil
}

// 关闭数据库实例
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	//逐一关闭数据库文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	for _, file := range db.oldFiles {
		err := file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// 追写到活跃文件中，但是加锁
func (db *DB) appendLogRecordWithLock(log *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(log)
}

// 追写到活跃数据文件中
func (db *DB) appendLogRecord(log *data.LogRecord) (*data.LogRecordPos, error) {

	//判断当前活跃文件是否存在，数据库在没有写入是没有文件生成
	//如果为空则初始化文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//编码logRecord结构体,并写入
	encRecord, size := data.EncodeLogRecord(log)
	//判断是否超过活跃文件的阈值，选择关闭数据文件打开新的数据文件
	if db.activeFile.Offset+size > db.options.DataFileSize {
		//当前文件进行数据持久化
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//当前活跃文件转化为旧数据文件
		db.oldFiles[db.activeFile.FileId] = db.activeFile

		//打开新数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//记录当前的偏移，用于当索引
	writerOffset := db.activeFile.Offset

	//写入文件
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//如果配置了SyncWrites，每次写入文件都进行持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writerOffset}
	return pos, nil
}

// 设置当前活跃数据文件
// 在访问此方法必须持有锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0

	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 校验数据库设置
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("datafile size <= 0")
	}
	return nil
}

// 加载数据库文件
func (db *DB) loadDataFiles() error {
	dirEntry, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	//遍历 .data结尾的文件，约定为数据文件
	for _, entry := range dirEntry {
		//判断为 .data结尾
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			//00001.data
			sqlitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(sqlitNames[0])
			//当前情况可能数据目录损坏
			if err != nil {
				return ErrDataDirectoryCorrupdated
			}
			fileIds = append(fileIds, fileId)
		}
	}

	//对文件id进行排序，从小到大依次加载
	sort.Ints(fileIds)

	db.fileIds = fileIds

	for i, fid := range fileIds {
		datafile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { //最后一个数据文件的话就是活跃数据文件
			db.activeFile = datafile
		} else { //其他纳入旧数据文件
			db.oldFiles[uint32(fid)] = datafile
		}
	}
	return nil
}

// 从数据文件加载内存索引，这里后续会改，直接逐一读数据文件太慢了。
func (db *DB) loadIndexFromDatafile() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	//查看是否发生过merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	//更新内存索引
	updateIndex := func(key []byte, typ data.LogRecordType, logRecordPos *data.LogRecordPos) error {
		var ok bool
		if typ == data.LogRecordDelete {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, logRecordPos)
		}
		if !ok {
			panic("fail to update index and startup")
		}
		return nil
	}

	//暂存事务数据
	transactionReocrds := make(map[uint64][]*data.TransactionReocrd)
	var currentSeqNo uint64

	//遍历所有的文件id
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)

		//如果小于nonmergeFileId，表示从hint文件加载
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var datafile *data.DataFile
		if fileId == db.activeFile.FileId {
			datafile = db.activeFile
		} else {
			datafile = db.oldFiles[fileId]
		}
		var Offset int64 = 0
		for {
			logRecord, size, err := datafile.ReadLogRecord(Offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			//将读取到的内存索引保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: Offset}

			//解析取出的key seq
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			//非事务处理直接更新
			if seqNo == nonTransactionSeqNo {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				//表示事务提交成功标识，加载到索引
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionReocrds[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionReocrds, seqNo)
				} else {
					logRecord.Key = realKey
					transactionReocrds[seqNo] = append(transactionReocrds[seqNo], &data.TransactionReocrd{
						Record: logRecord,
						Pos:    logRecordPos,
					})

				}
			}

			//更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			//递增offset
			Offset += size
		}

		//如果是活跃文件，记录Offset
		if i == len(db.fileIds)-1 {
			db.activeFile.Offset = Offset
		}
	}
	//	更新序列号
	db.seqNo = currentSeqNo
	return nil
}
