package bitcask_go

import "testing"

func TestStatGetFreeSize(t *testing.T) {
	free, err := AvailableDiskSize()
	t.Log(free / (1024 * 1024 * 1024))
	t.Log(err)
	t.Fail()
}
