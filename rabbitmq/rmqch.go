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

type RmqCh struct {
	ch        *amqp.Channel
	mtx       sync.RWMutex
	ntfClose  chan *amqp.Error ////生产者关闭，自己不关闭
	done      chan error       ////自己是生产者，自己关闭
	lastError string
}

func (c *RmqCh) Init(ch *amqp.Channel) {
	c.ch = ch
	c.ntfClose = make(chan *amqp.Error)
	c.done = make(chan error)
	c.ch.Qos(1, 0, false)
	c.ch.NotifyClose(c.ntfClose)
	go c.checkConnection()
}

func (c *RmqCh) Check() (err error) {
	if c.ch == nil {
		return errors.New("rmqch nil")
	}
	if c.ntfClose == nil || c.done == nil {
		return errors.New("rmqch chan closed")
	}
	return
}

func (c *RmqCh) checkConnection() {
	go func() {
		select {
		case err := <-c.ntfClose:
			amqp.Logger.Printf("rmqch check connection close %s", err)
			c.Close(RmqConnClose)
			break
		case <-c.done:
			amqp.Logger.Printf("rmqch check connection done")
			break
		}
	}()
}

func (c *RmqCh) Close(err string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.lastError = err
	if c.done != nil {
		close(c.done)
		c.done = nil
	}
	if c.ch != nil && !c.ch.IsClosed() {
		c.ch.Close()
	}
	c.ch = nil
}

func (c *RmqCh) IsClosed() bool {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	if c.ch == nil {
		return true
	} else if c.ch.IsClosed() {
		return true
	} else {
		return false
	}
}

// for publish "direct/exchangeName/key" or "topic/exchangeName/key" or "fanout/exchangeName" or "queueName"
// for consume "direct/exchangeName/route" or "topic/exchangeName/route" or "fanout/exchangeName" or "queueName"
func __parsePath(path string) (exchangeKind, exchange, queue, key string, err error) {
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

func (r *RmqCh) __prepareForPublish(exchangeKind, exchange, queue string) (err error) {
	err = r.Check()
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

func (r *RmqCh) Publish(ctx context.Context, path, correlationId, replyTo, body string) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	exchangeKind, exchange, queue, key, err := __parsePath(path)
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
// 死信队列 自动创建 名为：queue 加后缀 _dlx
func __parsePathForDlx(path string) (exchangeKind, exchange, queueDlx string, err error) {
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

func (r *RmqCh) __prepareForPublishDlx(exchangeKind, exchange, queueDlx string) (q amqp.Queue, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	err = r.ch.ExchangeDeclare(exchange, exchangeKind, true, false, false, false, nil)
	if err != nil {
		return
	}
	q, err = r.ch.QueueDeclare(queueDlx, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": exchange,
		// "x-mesage-ttl":86400000, //队列中消息的过期时间，单位为毫秒, 和消息pub时的过期时间取最小值起作用
		// "x-dead-letter-routing-key":queue, //死信交换机根据当前重新指定的routin key将消息重新路由到死信队列
		// "x-expires":86400000, //表示超过该设定的时间，队列将被自动删除,
		// "x-max-length",表示队列中能存储的最大消息数，超过该数值后，消息变为死信

	})
	return
}

func (r *RmqCh) PublishDlx(ctx context.Context, path, correlationId, replyTo, body string, delay time.Duration) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	exchangeKind, exchange, queueDlx, err := __parsePathForDlx(path)
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

func (r *RmqCh) __prepareForConsume(exchangeKind, exchange, queue, route string) (q amqp.Queue, err error) {
	err = r.Check()
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

func (r *RmqCh) Consume(path, queue, tag string) (msgs <-chan amqp.Delivery, done chan error, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	exchangeKind := ""
	exchange := ""
	route := ""
	if path != "" {
		exchangeKind, exchange, _, route, err = __parsePath(path)
		if err != nil {
			amqp.Logger.Printf("rmq error consume-parsepath:%s", err)
			return
		}
	}

	q, err := r.__prepareForConsume(exchangeKind, exchange, queue, route)
	if err != nil {
		amqp.Logger.Printf("rmq error consume-prepare:%s", err)
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
		amqp.Logger.Printf("rmq error consume:%s", err)
	}
	return
}

func (r *RmqCh) ConsumeDlx(path, queue, tag string) (msgs <-chan amqp.Delivery, done chan error, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	exchangeKind := ""
	exchange := ""
	queueDlx := ""
	if path != "" {
		exchangeKind, exchange, queueDlx, err = __parsePathForDlx(path)
		if err != nil {
			amqp.Logger.Printf("rmq error consume_dlx-parsepath:%s", err)
			return
		}
	}
	if queue == queueDlx {
		amqp.Logger.Printf("rmq error consume_dlx-parsepath: queue == queueDlx")
		err = errors.New("queue == queueDlx from path")
		return
	}
	q, err := r.__prepareForConsume(exchangeKind, exchange, queue, queueDlx)
	if err != nil {
		amqp.Logger.Printf("rmq error consume_dlx-prepare:%s", err)
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
		amqp.Logger.Printf("rmq error consume_dlx:%s", err)
	}
	return
}

func (r *RmqCh) Receive(ctx context.Context, msgs <-chan amqp.Delivery, callback func(msg *amqp.Delivery, err error) (ackOrReject bool, stop bool)) error {
	//--这一步很重要，Close设置r.done=nil 导致监听失败
	done := r.done
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			ackOrReject, stop := callback(&msg, nil)
			if ackOrReject {
				msg.Ack(false)
			} else {
				msg.Reject(true)
			}
			if stop {
				return nil
			}
		case <-done:
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
