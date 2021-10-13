package dqradix

import (
	"context"
	"testing"

	_is "github.com/matryer/is"
)

const (
	key = "qdradix_test"
)

func TestBasic(t *testing.T) {
	ctx := context.Background()
	is := _is.New(t)
	cl := NewForTesting(t, key)

	m1 := []byte("member1")
	m2 := []byte("member2")
	m3 := []byte("member3")

	is.NoErr(cl.ZRem(ctx, key, m1))

	c, err := cl.ZAddNX(ctx, key, 1, m1)
	is.NoErr(err)
	is.Equal(c, 1)

	c, err = cl.ZAddNX(ctx, key, 1, m1)
	is.NoErr(err)
	is.Equal(c, 0)

	is.NoErr(cl.ZRem(ctx, key, m1))

	c, err = cl.ZAddNX(ctx, key, 1, m1)
	is.NoErr(err)
	is.Equal(c, 1)

	c, err = cl.ZAddNX(ctx, key, 2, m1)
	is.NoErr(err)
	is.Equal(c, 0)

	c, err = cl.ZAddNX(ctx, key, 2, m2)
	is.NoErr(err)
	is.Equal(c, 1)

	c, err = cl.ZAddNX(ctx, key, 3, m3)
	is.NoErr(err)
	is.Equal(c, 1)

	res, err := cl.ZRangeByScore(ctx, key, 1, 3)
	is.NoErr(err)
	is.Equal(len(res), 3)
	for i := 0; i < 3; i++ {
		is.Equal(res[i].Member, [][]byte{m1, m2, m3}[i])
		is.Equal(res[i].Score, int64(i+1))
	}

	res, err = cl.ZRangeByScore(ctx, key, 2, 2)
	is.NoErr(err)
	is.Equal(len(res), 1)
}
