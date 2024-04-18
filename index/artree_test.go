package index

import (
	"bitcask-go/data"
	"bitcask-go/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestART_Put(t *testing.T) {
	art := NewART()
	art.Put(utils.GetTestKey(2), &data.LogRecordPos{1, 3})
	art.Put(utils.GetTestKey(8), &data.LogRecordPos{1, 3})
	art.Put(utils.GetTestKey(4), &data.LogRecordPos{1, 3})
	art.Put(utils.GetTestKey(3), &data.LogRecordPos{1, 3})
	art.Put(utils.GetTestKey(1), &data.LogRecordPos{1, 3})
	art.Put(utils.GetTestKey(4), &data.LogRecordPos{1, 3})
}

func TestART_GetPut(t *testing.T) {
	art := NewART()
	art.Put(utils.GetTestKey(2), &data.LogRecordPos{2, 3})
	art.Put(utils.GetTestKey(8), &data.LogRecordPos{1, 7})
	art.Put(utils.GetTestKey(4), &data.LogRecordPos{1, 4})
	art.Put(utils.GetTestKey(3), &data.LogRecordPos{5, 3})
	art.Put(utils.GetTestKey(1), &data.LogRecordPos{1, 3})
	art.Put(utils.GetTestKey(4), &data.LogRecordPos{1, 3})

	v1 := art.Get(utils.GetTestKey(2))
	v2 := art.Get(utils.GetTestKey(4))
	v3 := art.Get(utils.GetTestKey(3))
	t.Log(v1, v2, v3)
	assert.NotNil(t, v1)
	assert.NotNil(t, v2)
	assert.NotNil(t, v3)

	v4 := art.Get([]byte("wdsawdawdsaw"))
	assert.Nil(t, v4)

	art.Put(utils.GetTestKey(3), &data.LogRecordPos{5, 3})
	art.Put(utils.GetTestKey(3), &data.LogRecordPos{77, 3})
	art.Put(utils.GetTestKey(3), &data.LogRecordPos{256, 3123})
	v3 = art.Get(utils.GetTestKey(3))
	t.Log(v3)

	// t.Fail()

}

func TestART_Delete(t *testing.T) {
	art := NewART()
	d := art.Delete([]byte("NO Key"))
	assert.False(t, d)

	art.Put(utils.GetTestKey(3), &data.LogRecordPos{5, 3})
	d = art.Delete(utils.GetTestKey(3))
	assert.True(t, d)

	v3 := art.Get(utils.GetTestKey(3))
	assert.Nil(t, v3)

	d = art.Delete(utils.GetTestKey(3))
	assert.False(t, d)

	// t.Fail()
}

func TestART_Size(t *testing.T) {
	art := NewART()
	assert.Equal(t, art.Size(), 5)
	art.Put(utils.GetTestKey(2), &data.LogRecordPos{2, 3})
	art.Put(utils.GetTestKey(8), &data.LogRecordPos{1, 7})
	art.Put(utils.GetTestKey(4), &data.LogRecordPos{1, 4})
	art.Put(utils.GetTestKey(3), &data.LogRecordPos{5, 3})
	art.Put(utils.GetTestKey(1), &data.LogRecordPos{1, 3})
	art.Put(utils.GetTestKey(4), &data.LogRecordPos{1, 3})

	n := art.Size()
	assert.Equal(t, n, 5)
	// t.Fail()
}

func TestART_It(t *testing.T) {
	art := NewART()

	art.Put(utils.GetTestKey(2), &data.LogRecordPos{2, 3})
	art.Put(utils.GetTestKey(8), &data.LogRecordPos{1, 7})
	art.Put(utils.GetTestKey(4), &data.LogRecordPos{1, 4})
	art.Put(utils.GetTestKey(3), &data.LogRecordPos{5, 3})
	art.Put(utils.GetTestKey(1), &data.LogRecordPos{1, 3})
	art.Put(utils.GetTestKey(4), &data.LogRecordPos{1, 3})
	ait := art.Iterator(false)
	for ait.Rewind(); ait.Valid(); ait.Next() {
		t.Log(string(ait.Key()))
		t.Log(ait.Value())
	}
	// t.Fail()
}
