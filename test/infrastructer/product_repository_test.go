package infrastructer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdd(t *testing.T) {

	t.Run("Add fonksiyonu testi ", func(t *testing.T) {
		actual := Add(5, 10)
		assert.Equal(t, 15, actual)
	})

}

func Add(x int, y int) int {
	return x + y
}
