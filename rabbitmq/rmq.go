package rabbitmq

import (
	"context"
	"errors"
	"fmt"
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
	urls []string
	conn *amqp.Connection
	// ch   *amqp.Channel
	ch        *RmqCh           ////default channel
	ntfClose  chan *amqp.Error ////生产者关闭，自己不关闭
	done      chan error       ////自己是生产者，自己关闭
	mtx       sync.Mutex
	mtxCh     sync.Mutex
	lastError string
	channels  map[string]*RmqCh
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
	r.channels = map[string]*RmqCh{}
	return nil
}

func (r *Rmq) Connect() (err error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if r.conn != nil {
		return nil
	} else {
		//--------close------
		func() {
			r.mtxCh.Lock()
			defer r.mtxCh.Unlock()
			for _, channel := range r.channels {
				channel.Close("")
			}
			clear(r.channels)
			if r.ch != nil {
				r.ch = nil
				r.ch.Close("")
			}
		}()
		//------------------
		r.ntfClose = make(chan *amqp.Error)
		r.done = make(chan error)

		for i := 0; i < len(r.urls); i++ {
			url := r.urls[i]
			conn, e := amqp.Dial(url)
			if e != nil {
				err = e
				amqp.Logger.Printf("rmq error dial %d %s : %s %p", i, url, err, conn)
				continue
			} else {
				err = nil
				r.conn = conn
				r.conn.NotifyClose(r.ntfClose)
				break
			}
		}
		if err != nil {
			return
		}
	}
	r.checkConnection()
	amqp.Logger.Printf("rmq connected")
	return
}

func (r *Rmq) Channel(name string) (rch *RmqCh, err error) {
	err = r.Connect()
	if err != nil {
		return
	}
	r.mtxCh.Lock()
	defer r.mtxCh.Unlock()
	channel := r.channels[name]
	// if channel != nil && !channel.IsClosed() {
	if channel != nil {
		return channel, nil
	}
	//-------------------
	ch, err := r.conn.Channel()
	if err != nil {
		amqp.Logger.Printf("rmq error channel %s", name)
		return nil, err
	}
	//-------------------
	rch = new(RmqCh)
	rch.Init(ch)
	r.channels[name] = rch
	amqp.Logger.Printf("rmq channel succ %s", name)
	return
}

func (r *Rmq) checkConnection() {
	// con := r.conn
	go func() {
		select {
		case err := <-r.ntfClose:
			amqp.Logger.Printf("rmq check connection close %s", err)
			// if r.conn != nil && con == r.conn {
			r.Close(RmqConnClose)
			// }
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
	if r.done != nil {
		close(r.done)
		r.done = nil
	}
	func() {
		r.mtxCh.Lock()
		defer r.mtxCh.Unlock()
		if r.ch != nil {
			r.ch.Close(errStr)
			r.ch = nil
		}
		for _, channel := range r.channels {
			channel.Close(errStr)
		}
		clear(r.channels)
	}()
	if !r.conn.IsClosed() {
		r.conn.Close()
	}
	r.conn = nil
}

// -----------------
func (r *Rmq) Check() (err error) {
	err = r.Connect()
	if err != nil {
		return
	}
	r.mtxCh.Lock()
	defer r.mtxCh.Unlock()
	if r.ch == nil {
		ch, err := r.conn.Channel()
		if err != nil {
			amqp.Logger.Printf("rmq error check channel")
			return err
		}
		r.ch = new(RmqCh)
		r.ch.Init(ch)
	}
	return
}

func (r *Rmq) Publish(ctx context.Context, path, correlationId, replyTo, body string) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.Publish(ctx, path, correlationId, replyTo, body)
}

func (r *Rmq) PublishDlx(ctx context.Context, path, correlationId, replyTo, body string, delay time.Duration) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.PublishDlx(ctx, path, correlationId, replyTo, body, delay)
}

func (r *Rmq) Consume(path, queue, tag string) (msgs <-chan amqp.Delivery, done chan error, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.Consume(path, queue, tag)
}

func (r *Rmq) ConsumeDlx(path, queue, tag string) (msgs <-chan amqp.Delivery, done chan error, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.ConsumeDlx(path, queue, tag)
}

func (r *Rmq) Receive(ctx context.Context, msgs <-chan amqp.Delivery, callback func(msg *amqp.Delivery, err error) (ackOrReject bool, stop bool)) error {
	err := r.Check()
	if err != nil {
		return err
	}
	return r.ch.Receive(ctx, msgs, callback)
}

func (r *Rmq) Rpc(ctx context.Context, queue, correlationId, replyTo, body string) (rst []byte, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.Rpc(ctx, queue, correlationId, replyTo, body)
}

func (r *Rmq) Stream(ctx context.Context, queue, replyTo string, streamFunc func(isReq bool, data *RmqStreamData) (stop bool)) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.Stream(ctx, queue, replyTo, streamFunc)
}

func (r *Rmq) QueuePurge(queue string) (count int, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.QueuePurge(queue)
}

func (r *Rmq) QueueDelete(queue string, isUnused, isEmpty bool) (count int, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.QueueDelete(queue, isUnused, isEmpty)
}

func (r *Rmq) QueueUnbind(queue, key, exchange string) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.QueueUnbind(queue, key, exchange)
}

func (r *Rmq) ExchangeDelete(name string, isUnused bool) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.ExchangeDelete(name, isUnused)
}

func (r *Rmq) ExchangeUnbind(destination, key, source string) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	return r.ch.ExchangeUnbind(destination, key, source)
}
