# delayq

`delayq` is a Go implementation of a redis based delayed queue as described 
the [Redis In Action](https://redis.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-4-task-queues/6-4-2-delayed-tasks/) ebook. 
Messages added to the queue have a deadline, the library will "pop" messages off the queue when the deadline is reached. 

It uses a redis sorted set under the hood with messages sorted (scored) by timestamp. Messages can be added via the convenience function `delayq.Add` or
via `delayq.AddMsg` for more control. The blocking method `delayq.Dequeue ` pops messages as their deadlines are reached calling the callback
function for each and deleting the message from the queue if the callback doesn't return an error. `delayq.Dequeue` returns on first error encountered. It
is sufficient to call `delayq.Dequeue` in a loop, logging errors and possibly doing backoff. 

Notes:
 - redis sorted set scores are implemented as doubles so there is a loss of precision of popped message timestamps.
 - The combination of message ID and Data fields must be unique. `ErrExists` is returned if a message already exists. 
 - The main `delayq` package is decoupled from any redis client implementation via the `delayq.Redis` interface. 
 - A `radix` client implementation is provided in the `dqradix` package.
 
## Usage

```go
// Redis client (radix)
radixClient := radix.Dial(ctx, "tcp", "localhost:6379")

// Delayq q instance
q := delayq.New(dqradix.New(radixClient), "z)

// Add is a convenience function that adds a message to the queue with a delay; a new random UUID is generated automatically andr returned.
uuid, err = delayq.Add(ctx, []byte("my message"), time.Minute)

// AddMsg adds the specified message to the queue. 
err = delayq.AddMsg(ctx, &delayq.Msg{
    ID: uuid.New(),
    Deadline: time.Now().Add(time.Minute),
    Data: []byte("some other message"), 
  })

// Define a callback for popped messages.
callback := func(msg *delayq.Msg) error {
  fmt.Printf("Popped message: %v", msg)
  return nil
}

// Run dequeue in a loop, logging errors and backing off.
for {
    if ctx.Err() != nil { 
       // The root context was canceled, we are done.
       break
    }
    
    err := delayq.Dequeue(ctx, callback)
    fmt.Printf("dequeue error: %v", err)
    time.Sleep(time.Second) // Backoff
}
```
