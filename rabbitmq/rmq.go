package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const RmqConnClose = "connect_close"
const RmqCtxDone = "ctx_done"
const RmqPathSep = "/"
const RmqExChangeDirect = "direct"
const RmqExChangeTopic = "topic"
const RmqExChangeFanout = "fanout"

type Rmq struct {
	urls      []string
	conn      *amqp.Connection
	ch        *amqp.Channel
	connClose chan *amqp.Error
	done      chan error
	mtx       sync.Mutex
	lastError string
}
type RmqLogger struct {
}

func (*RmqLogger) Printf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}

func NewRmq(urls []string, logger amqp.Logging) (r *Rmq, err error) {
	r = new(Rmq)
	err = r.Init(urls)
	if err != nil {
		r = nil
	}
	if logger != nil {
		amqp.SetLogger(logger)
	} else {
		amqp.SetLogger(new(RmqLogger))
	}
	return
}

func (r *Rmq) Init(urls []string) error {
	if len(urls) == 0 {
		amqp.Logger.Printf("rmq error init urls empty")
		return errors.New("urls empty")
	}
	r.urls = urls
	return nil
}

func (r *Rmq) Connect() (err error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if r.conn != nil {
		return nil
	} else {
		r.connClose = make(chan *amqp.Error)
		r.done = make(chan error)
		for i := 0; i < len(r.urls); i++ {
			url := r.urls[i]
			r.conn, err = amqp.Dial(url)
			if err != nil {
				amqp.Logger.Printf("rmq error dial %d %s : %s", i, url, err)
				continue
			}
			r.conn.NotifyClose(r.connClose)
			break
		}
		if err != nil {
			return
		}
	}
	r.checkConnection()
	r.ch, err = r.conn.Channel()
	if err != nil {
		amqp.Logger.Printf("rmq error channel")
		return
	}
	amqp.Logger.Printf("rmq connected")
	return
}

func (r *Rmq) checkConnection() {
	con := r.conn
	go func() {
		select {
		case <-r.connClose:
			amqp.Logger.Printf("rmq check connection close")
			if con == r.conn && con != nil {
				r.Close(RmqConnClose)
			}
			break
		case <-r.done:
			amqp.Logger.Printf("rmq check connection done")
			break
		}
	}()
}

func (r *Rmq) Close(errStr string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.lastError = errStr
	if r.conn == nil {
		return
	}
	if !r.conn.IsClosed() {
		r.conn.Close()
		r.conn = nil
	}
	close(r.done)
}

// for publish "direct/exchangeName/key" or "topic/exchangeName/key" or "fanout/exchangeName" or "queueName"
// for consume "direct/exchangeName/route" or "topic/exchangeName/route" or "fanout/exchangeName" or "queueName"
func (r *Rmq) __parsePath(path string) (exchangeKind, exchange, queue, key string, err error) {
	tmps := []string{RmqExChangeDirect, RmqExChangeTopic, RmqExChangeFanout}
	rts := strings.Split(path, RmqPathSep)
	if len(rts) == 1 {
		key = rts[0]
		queue = key
	} else if len(rts) == 2 {
		exchangeKind = rts[0]
		exchange = rts[1]
		if exchangeKind != RmqExChangeFanout {
			err = errors.New("route empty")
			return
		}
	} else if len(rts) == 3 {
		exchangeKind = rts[0]
		exchange = rts[1]
		key = rts[2]
		valid := false
		for _, v := range tmps {
			if v == exchangeKind {
				valid = true
				break
			}
		}
		if !valid {
			err = errors.New("exchange type invalid")
			return
		}
		if exchangeKind == RmqExChangeFanout {
			key = ""
		} else {
			if key == "" {
				err = errors.New("key empty")
				return
			}
		}
	} else {
		err = errors.New("path invalid: need len = 1|3")
		return
	}
	// amqp.Logger.Printf("rmq __parse_path succ %s %s %s %s", exchangeKind, exchange, queue, key)
	return
}

func (r *Rmq) __prepareForPublish(exchangeKind, exchange, queue string) (err error) {
	err = r.Connect()
	if err != nil {
		return
	}
	if exchangeKind != "" {
		err = r.ch.ExchangeDeclare(exchange, exchangeKind, true, false, false, false, nil)
		if err != nil {
			return
		}
	}
	if queue != "" {
		_, err = r.ch.QueueDeclare(queue, true, false, false, false, nil)
	}
	return
}

func (r *Rmq) Publish(ctx context.Context, path, correlationId, replyTo, body string) (err error) {
	exchangeKind, exchange, queue, key, err := r.__parsePath(path)
	if err != nil {
		amqp.Logger.Printf("rmq error publish-parsepath:%s", err)
		return
	}
	err = r.__prepareForPublish(exchangeKind, exchange, queue)
	if err != nil {
		amqp.Logger.Printf("rmq error publish-prepare:%s", err)
		return
	}

	err = r.ch.PublishWithContext(ctx, exchange, key, false, false, amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		ContentType:   "text/plain",
		ReplyTo:       replyTo,
		CorrelationId: correlationId,
		Body:          []byte(body),
	})
	if err != nil {
		amqp.Logger.Printf("rmq error publish:%s", err)
	} else {
		// amqp.Logger.Printf("rmq publish succ :%s %s", exchange, key)
	}
	return
}

// for publish_dlx "exchangType/exchangeName/queue" return queue_dlx
func (r *Rmq) __parsePathForDlx(path string) (exchangeKind, exchange, queueDlx string, err error) {
	rts := strings.Split(path, RmqPathSep)
	if len(rts) == 3 {
		exchangeKind = rts[0]
		exchange = rts[1]
		queueDlx = rts[2] + "_dlx"
	} else {
		err = errors.New("path invalid")
	}
	return
}

func (r *Rmq) __prepareForPublishDlx(exchangeKind, exchange, queue string) (q amqp.Queue, err error) {
	err = r.Connect()
	if err != nil {
		return
	}
	err = r.ch.ExchangeDeclare(exchange, exchangeKind, true, false, false, false, nil)
	if err != nil {
		return
	}
	q, err = r.ch.QueueDeclare(queue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": exchange,
		// "x-mesage-ttl":86400000,
	})
	return
}

func (r *Rmq) PublishDlx(ctx context.Context, path, correlationId, replyTo, body string, delay time.Duration) (err error) {
	exchangeKind, exchange, queueDlx, err := r.__parsePathForDlx(path)
	if err != nil {
		amqp.Logger.Printf("rmq error publish_dlx-parsepath:%s", err)
		return
	}
	q, err := r.__prepareForPublishDlx(exchangeKind, exchange, queueDlx)
	if err != nil {
		amqp.Logger.Printf("rmq error publish_dlx-prepare:%s", err)
		return
	}

	duration := time.Duration(q.Messages+1) * delay
	expiration := fmt.Sprintf("%d", int(duration.Milliseconds()))

	err = r.ch.PublishWithContext(ctx, "", queueDlx, false, false, amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		ContentType:   "text/plain",
		ReplyTo:       replyTo,
		CorrelationId: correlationId,
		Body:          []byte(body),
		Timestamp:     time.Now(),
		Expiration:    expiration,
	})
	if err != nil {
		amqp.Logger.Printf("rmq error publish_dlx:%s", err)
	} else {
		// amqp.Logger.Printf("rmq publish_dlx succ :%s %s", exchange, key)
	}
	return
}

func (r *Rmq) __prepareForConsume(exchangeKind, exchange, queue, route string) (q amqp.Queue, err error) {
	err = r.Connect()
	if err != nil {
		return
	}
	q, err = r.ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		amqp.Logger.Printf("rmq error prepare_for_consume queue_declare:%s", err)
		return
	} else {
		// amqp.Logger.Printf("rmq prepare_for_consume queue_declare succ :%s", queue)
	}

	if exchangeKind != "" {
		err = r.ch.ExchangeDeclare(exchange, exchangeKind, true, false, false, false, nil)
		if err != nil {
			amqp.Logger.Printf("rmq error prepare_for_consume exchange_declare:%s", err)
			return
		} else {
			// amqp.Logger.Printf("rmq prepare_for_consume exchange_declare succ :%s %s", exchangeKind, exchange)
		}
		err = r.ch.QueueBind(q.Name, route, exchange, false, nil)
		if err != nil {
			amqp.Logger.Printf("rmq error prepare_for_consume queue_bind:%s", err)
			return
		} else {
			// amqp.Logger.Printf("rmq prepare_for_consume queue_bind succ :%s %s %s", q.Name, route, exchange)
		}
	}
	return
}

func (r *Rmq) ReceiveOne(ctx context.Context, path, queue, correlationId string) (body []byte, err error) {
	exchangeKind := ""
	exchange := ""
	route := ""
	if path != "" {
		exchangeKind, exchange, _, route, err = r.__parsePath(path)
		if err != nil {
			amqp.Logger.Printf("rmq error receive_one parsepath:%s", err)
			return
		}
	}

	q, err := r.__prepareForConsume(exchangeKind, exchange, queue, route)
	if err != nil {
		amqp.Logger.Printf("rmq error receive_one prepare:%s", err)
		return
	}
	msgs, err := r.ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	for {
		select {
		case msg := <-msgs:
			if msg.CorrelationId == correlationId {
				msg.Ack(false)
				body = msg.Body
				amqp.Logger.Printf("rmq receive_one ok %s", correlationId)
				break
			} else {
				amqp.Logger.Printf("rmq receive_one reject continue")
				msg.Reject(true)
			}
		// case <-r.connClose:
		// 	err = errors.New("conn Closed")
		// 	break
		case <-r.done:
			err = errors.New(r.lastError)
			amqp.Logger.Printf("rmq error receive_one done:%s", err)
		case <-ctx.Done():
			err = errors.New(RmqCtxDone)
			amqp.Logger.Printf("rmq error receive_one ctx_down:%s", err)
			break
		}
	}
}

func (r *Rmq) Rpc(ctx context.Context, path, correlationId, replyTo, body string) (rst []byte, err error) {
	err = r.Publish(ctx, path, correlationId, replyTo, body)
	if err != nil {
		amqp.Logger.Printf("rmq error publish_and_receive-publish:%s", err)
		return
	}
	rst, err = r.ReceiveOne(ctx, path, replyTo, correlationId)
	if err != nil {
		amqp.Logger.Printf("rmq error publish_and_receive-receive:%s", err)
	}
	return
}

func (r *Rmq) GenConsumer(path, queue, tag string) (msgs <-chan amqp.Delivery, done chan error, err error) {
	exchangeKind := ""
	exchange := ""
	route := ""
	if path != "" {
		exchangeKind, exchange, _, route, err = r.__parsePath(path)
		if err != nil {
			amqp.Logger.Printf("rmq error genconsumer-parsepath:%s", err)
			return
		}
	}

	q, err := r.__prepareForConsume(exchangeKind, exchange, queue, route)
	if err != nil {
		amqp.Logger.Printf("rmq error genconsumer-prepare:%s", err)
		return
	}
	msgs, err = r.ch.Consume(
		q.Name, // queue
		tag,    // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	done = r.done
	if err != nil {
		amqp.Logger.Printf("rmq error genconsumer:%s", err)
	}
	return
}

func (r *Rmq) GenConsumerDlx(path, queue, tag string) (msgs <-chan amqp.Delivery, done chan error, err error) {
	exchangeKind := ""
	exchange := ""
	queueDlx := ""
	if path != "" {
		exchangeKind, exchange, queueDlx, err = r.__parsePathForDlx(path)
		if err != nil {
			amqp.Logger.Printf("rmq error genconsumer_dlx-parsepath:%s", err)
			return
		}
	}
	if queue == queueDlx {
		amqp.Logger.Printf("rmq error genconsumer_dlx-parsepath: queue == queueDlx")
		err = errors.New("queue == queueDlx from path")
		return
	}
	q, err := r.__prepareForConsume(exchangeKind, exchange, queue, queueDlx)
	if err != nil {
		amqp.Logger.Printf("rmq error genconsumer_dlx-prepare:%s", err)
		return
	}
	msgs, err = r.ch.Consume(
		q.Name, // queue
		tag,    // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	done = r.done
	if err != nil {
		amqp.Logger.Printf("rmq error genconsumer_dlx:%s", err)
	}
	return
}

func (r *Rmq) Receive(ctx context.Context, msgs <-chan amqp.Delivery, callback func(msg *amqp.Delivery, err error) (ackOrReject bool, stop bool)) error {
	for {
		select {
		case msg := <-msgs:
			ackOrReject, stop := callback(&msg, nil)
			if ackOrReject {
				msg.Ack(false)
			} else {
				msg.Reject(true)
			}
			if stop {
				return nil
			}
		case <-r.done:
			if r.lastError == RmqConnClose {
				amqp.Logger.Printf("rmq error receive:%s", r.lastError)
				return errors.New(r.lastError)
			}
			amqp.Logger.Printf("rmq done receive")
			return nil
		case <-ctx.Done():
			amqp.Logger.Printf("rmq ctx_done receive")
			return nil
		}
	}
}

func (r *Rmq) QueuePurge(queue string) (count int, err error) {
	err = r.Connect()
	if err != nil {
		return
	}
	count, err = r.ch.QueuePurge(queue, false)
	return
}

func (r *Rmq) QueueDelete(queue string, isUnused, isEmpty bool) (count int, err error) {
	err = r.Connect()
	if err != nil {
		return
	}
	count, err = r.ch.QueueDelete(queue, isUnused, isEmpty, false)
	return
}

func (r *Rmq) QueueUnbind(queue, key, exchange string) (err error) {
	err = r.Connect()
	if err != nil {
		return
	}
	err = r.ch.QueueUnbind(queue, key, exchange, nil)
	return
}

func (r *Rmq) ExchangeDelete(name string, isUnused bool) (err error) {
	err = r.Connect()
	if err != nil {
		return
	}
	err = r.ch.ExchangeDelete(name, isUnused, false)
	return
}

func (r *Rmq) ExchangeUnbind(destination, key, source string) (err error) {
	err = r.Connect()
	if err != nil {
		return
	}
	err = r.ch.ExchangeUnbind(destination, key, source, false, nil)
	return
}
