package delayq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/corverroos/delayq/dqradix"
	_is "github.com/matryer/is"
)

const key = "delayq"

func TestRadix(t *testing.T) {
	testLogic(t, dqradix.NewForTesting(t, key))
}

func testLogic(t *testing.T, cl Redis) {
	is := _is.New(t)
	ctx := context.Background()
	t0 := time.Now()
	var sleeps []time.Duration

	now = func() time.Time {
		return t0
	}

	sleep = func(d time.Duration) {
		t0 = t0.Add(d)
		sleeps = append(sleeps, d)
	}

	t.Cleanup(func() {
		now = time.Now
		sleep = time.Sleep
	})

	q := New(cl, key)

	total := 10

	makeMsg := func(i int) *Msg {
		return &Msg{
			ID:       fmt.Sprint(i),
			Data:     []byte(fmt.Sprint(i)),
			Deadline: t0.Add(time.Millisecond * 200 * time.Duration(i)),
		}
	}

	ms := func(m *Msg) string {
		s, _ := json.Marshal(m)
		return string(s)
	}

	for i := total - 1; i >= 0; i-- {
		err := q.AddMsg(ctx, makeMsg(i))
		is.NoErr(err)
	}

	var i int
	err := q.Dequeue(ctx, func(msg *Msg) error {
		is.Equal(ms(msg), ms(makeMsg(i)))
		i++
		if i >= total {
			return io.EOF
		}
		return nil
	})
	is.Equal(err, io.EOF)

	// Everything deleted
	res, err := cl.ZRangeByScore(ctx, key, 0, 1000)
	is.NoErr(err)
	is.Equal(len(res), 0)
}
