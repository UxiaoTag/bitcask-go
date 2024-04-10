package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTestKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		str := string(GetTestKey(i))
		t.Log(str)
		assert.NotNil(t, str)
	}
}

func TestRandomValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		str := string(RandomValue(10))
		t.Log(str)
		assert.NotNil(t, str)
	}
}
