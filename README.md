# mq based github.com/rabbitmq/amqp091-go

## Urls and connect
```golang 
1. support multi urls, supposed that these urls belong to one cluster
2. try next url if previous connecting err in function Connect until the end, not retry looply
3. commonly you need to recall Connect or other funcs in your code while current connection closed 
```

## Param path with several forms: 
```
1. consist of exchageType，exchangeName，key(for publish) or route(for consume) and seperated by '/', in order to simply call
2. path is queue name if not use exchange
```

```golang
// path : "direct/exchangeName/key" or "topic/exchangeName/key" or "fanout/exchangeName" or "queueName"
func (r *Rmq) Publish(ctx context.Context, path, correlationId, replyTo, body string) (err error){}

// path : "exchangType/exchangeName/queue" return exchangType,exchangeName,queue_dlx
// 死信队列 自动创建 名为：queue 加后缀 _dlx
func (r *Rmq) PublishDlx(ctx context.Context, path, correlationId, replyTo, body string, delay time.Duration) (err error) {}

//path :"direct/exchangeName/route" or "topic/exchangeName/route" or "fanout/exchangeName" or "queueName"
func (r *Rmq) GenConsumer(path, queue, tag string) (msgs <-chan amqp.Delivery, done chan error, err error){}

//path :"direct/exchangeName/route" or "topic/exchangeName/route" or "fanout/exchangeName" or "queueName"
func (r *Rmq) GenConsumerDlx(path, queue, tag string) (msgs <-chan amqp.Delivery, done chan error, err error){}
```

## Rmq and RmqCh
```golang
1. rmq hold amqp.Connect
2. rmqch hold amqp.Channel, you can get a rmqch by func (r *Rmq) Channel(name string) (rch *RmqCh, err error)
3. rmq cache rmqch in a map , rmqch reuse one Connect from rmq
4. rmq also hold a default rmqch inorder to call simply. while you only need one channel you can call these functions below.
5. func of rmq like Publish,Consume,Rpc ... , support auto connect while current connection is closed or not connected.
6. while connection closed, you need to call rmq.Channel(name) to generate new rmqch. 
```

## Rpc
```golang
1. do not support exchange and consume with param exclusive: true
2. identify messages by correlationID and auto ignore the other messages until received the correct msg then return
```

## Stream
```golang
1. do not support exchange , consume with param exclusive: true
2. identify messages by correlationID and auto ignore the other messages until received the correct msg then return
3. param callback has two functions : fill request correlationId,body and receive response body for each rpc
4. continuously rpc, until callback return true then return
```

## DLX testing feature
```golang
1. expiration of msg = (msgCount + 1) * delay
2. try testDlx() to known more
//----------------------
func (r *RmqCh) PublishDlx(ctx context.Context, path, correlationId, replyTo, body string, delay time.Duration) {
    ...
    duration := time.Duration(q.Messages+1) * delay
    expiration := fmt.Sprintf("%d", int(duration.Milliseconds()))
    ...
    err = r.ch.PublishWithContext(ctx, "", queueDlx, false, false, amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		ContentType:   "text/plain",
		ReplyTo:       replyTo,
		CorrelationId: correlationId,
		Body:          []byte(body),
		Timestamp:     time.Now(),
		Expiration:    expiration,
	})
    ...
}
```
## Todo
```golang
1. maybe support input more params of original rabbitmq and keep simple
2. maybe auto reconnect
```
