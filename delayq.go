// Package delayq provides an implementation of a redis based delay queue as described in
// https://redis.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-4-task-queues/6-4-2-delayed-tasks/.
package delayq

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrExists = errors.New("element already exists")

	now   = time.Now   // Time.Now aliased for testing.
	sleep = time.Sleep // time.Sleep aliased for testing.
)

// Redis abstracts a redis client implementation.
type Redis interface {
	// ZAddNX adds a member/element to a sorted set if it doesn't exist. See https://redis.io/commands/ZADD.
	ZAddNX(ctx context.Context, key string, score float64, member []byte) (int, error)

	// ZRem removes the member from the sorted set if it exists.
	ZRem(ctx context.Context, key string, member []byte) error

	// ZRangeByScore returns a range of elements WITH SCORES between (inclusive) the min and max
	// from a sorted set. See https://redis.io/commands/zrange.
	ZRangeByScore(ctx context.Context, key string, min, max float64) ([]struct {
		Member []byte
		Score  float64
	}, error)
}

type Msg struct {
	ID       string    `json:"id"`
	Data     []byte    `json:"data"`
	Deadline time.Time `json:"-"`
}

type options struct {
	PollPeriod time.Duration
}

func WithPollPeriod(d time.Duration) func(*options) {
	return func(o *options) {
		o.PollPeriod = d
	}
}

func New(cl Redis, name string) *Queue {
	return &Queue{
		name: name,
		cl:   cl,
	}
}

// Queue provides the main abstraction of a redis delay queue.
type Queue struct {
	name string
	cl   Redis
}

// Dequeue blocks and dequeues messages as their deadlines are reached until the first error. It always returns a non-nil error.
// Calling Dequeue from multiple goroutines will result in duplicate deliveries.
func (q *Queue) Dequeue(ctx context.Context, handler func(*Msg) error, opts ...func(*options)) error {
	o := options{
		PollPeriod: time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		nextPoll := now().Add(o.PollPeriod)

		el, err := q.cl.ZRangeByScore(ctx, q.name, 0, float64(nextPoll.UnixNano()))
		if err != nil {
			return err
		}

		for i := 0; i < len(el); i++ {
			var m Msg
			if err := json.Unmarshal(el[i].Member, &m); err != nil {
				return err
			}

			n := now()
			m.Deadline = time.Unix(0, int64(el[i].Score))
			if m.Deadline.After(n) {
				sleep(m.Deadline.Sub(n))
			}

			if err := handler(&m); err != nil {
				return err
			}

			if err := q.cl.ZRem(ctx, q.name, el[i].Member); err != nil {
				return err
			}
		}

		sleep(nextPoll.Sub(now()))
	}
}

func (q *Queue) Add(ctx context.Context, data []byte, delay time.Duration) (string, error) {
	id := uuid.NewString()

	err := q.AddMsg(ctx, &Msg{ID: id, Data: data, Deadline: now().Add(delay)})
	if err != nil {
		return "", err
	}

	return id, nil
}

func (q *Queue) AddMsg(ctx context.Context, msg *Msg) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	count, err := q.cl.ZAddNX(ctx, q.name, float64(msg.Deadline.UnixNano()), b)
	if err != nil {
		return err
	} else if count == 0 {
		return ErrExists
	}

	return nil
}
