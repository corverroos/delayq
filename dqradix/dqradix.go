// Package dqradix provides a delayq Redis implementation based on the radix v4 redis client.
package dqradix

import (
	"context"
	"errors"
	"strconv"
	"testing"

	_is "github.com/matryer/is"
	"github.com/mediocregopher/radix/v4"
)

func New(cl radix.Client) Client {
	return Client{cl: cl}
}

type Client struct {
	cl radix.Client
}

func (c Client) ZAddNX(ctx context.Context, key string, score float64, member []byte) (int, error) {
	var count int
	err := c.cl.Do(ctx, radix.FlatCmd(&count, "ZADD", key, "NX", score, member))
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (c Client) ZRem(ctx context.Context, key string, member []byte) error {
	return c.cl.Do(ctx, radix.Cmd(nil, "ZREM", key, string(member)))
}

func (c Client) ZRangeByScore(ctx context.Context, key string, min, max float64) ([]struct {
	Member []byte
	Score  float64
}, error) {

	var res [][]byte
	err := c.cl.Do(ctx, radix.FlatCmd(&res, "ZRANGEBYSCORE", key, min, max, "WITHSCORES"))
	if err != nil {
		return nil, err
	}

	if len(res)%2 != 0 {
		return nil, errors.New("unexpeted number of results")
	}

	var (
		tuples []struct {
			Member []byte
			Score  float64
		}
		tuple struct {
			Member []byte
			Score  float64
		}
	)

	for i := 0; i < len(res); i++ {
		if i%2 == 0 {
			tuple.Member = res[i]
			continue
		}

		f, err := strconv.ParseFloat(string(res[i]), 64)
		if err != nil {
			return nil, err
		}

		tuple.Score = f
		tuples = append(tuples, tuple)
	}

	return tuples, nil
}

func NewForTesting(t *testing.T, prefix string) Client {
	is := _is.New(t)

	ctx := context.Background()

	c, err := radix.Dial(ctx, "tcp", "127.0.0.1:6379")
	is.NoErr(err)

	clean := func() {
		var res []string
		is.NoErr(c.Do(ctx, radix.Cmd(&res, "KEYS", prefix)))
		for _, b := range res {
			is.NoErr(c.Do(ctx, radix.Cmd(nil, "DEL", b)))
		}
	}

	clean()
	t.Cleanup(func() {
		clean()
		is.NoErr(c.Close())
	})

	return New(c)
}
