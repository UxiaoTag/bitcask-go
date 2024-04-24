package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty               = errors.New("key is empty")
	ErrIndexUpdateFail          = errors.New("failed index update")
	ErrKeyNotFound              = errors.New("key not found in database")
	ErrNoDataFile               = errors.New("not found datafile")
	ErrDataDirectoryCorrupdated = errors.New("the database directory maybe corrupted")
	ErrExceedMaxBatchNum        = errors.New("exceed max batch num")
	ErrMergeIsProgress          = errors.New("merge is progress")
	ErrDatabaseIsUsing          = errors.New("the database directory is used by another process")
	ErrMergeRatioUnreached      = errors.New("the merge ratio has not reach the option")
	ErrNoFreeSpaceForMerge      = errors.New("the disk no have free space to merge")
)
