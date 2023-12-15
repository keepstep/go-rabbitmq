package main

import (
	"context"
	"fmt"
	"time"

	rmq "github.com/keepstep/mq/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func Log(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}

func test() {
	urls := []string{
		"amqp://keep:keep@localhost:7672/aaaa",
		"amqp://keep:keep@localhost:8672/aaaa",
		"amqp://keep:keep@localhost:6672/aaaa",
	}
	r, err := rmq.NewRmq(urls, nil)
	if err != nil {
		Log("NewRmq err %s", err)
		return
	}
	err = r.Connect()
	if err != nil {
		Log("Connect err %s", err)
		return
	}
	// path := "direct/exchange_direct/dog_direct"
	// queue := "dog_direct"
	// path := "fanout/exchange_fanout/dog_fanout"
	// queue := "dog_fanout"
	path := "topic/exchange_topic/dog_topic"
	queue := "dog_topic"

	replyTo := "pig"
	timeDelay := time.Duration(1)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)
	msgs, _, err := r.GenConsumer(path, queue, "")
	if err != nil {
		Log("GenConsumer err %s", err)
		return
	}

	go func() {
		i := 100
		for {
			select {
			case <-ctx.Done():
				Log("publish ctx done %d", i)
				break
			case <-time.After(time.Second * timeDelay):
				i++
				id := fmt.Sprintf("%d", i)
				err := r.Publish(ctx, path, id, replyTo, "123")
				if err != nil {
					break
				} else {
					Log("publish succ %d", i)
				}
			}
		}
	}()

	err = r.Receive(ctx, msgs, func(msg *amqp.Delivery, err error) (ackOrReject bool, stop bool) {
		if err != nil {
			Log("receive error %s", err)
		} else {
			id := msg.CorrelationId
			Log("receive %s : %s\n", id, string(msg.Body))
		}
		return true, false
	})
	if err != nil {
		Log("receive err %s", err)
	}
	Log("test over")
}

func testDlx() {
	urls := []string{
		"amqp://keep:keep@localhost:7672/aaaa",
		"amqp://keep:keep@localhost:8672/aaaa",
		"amqp://keep:keep@localhost:6672/aaaa",
	}
	r, err := rmq.NewRmq(urls, nil)
	if err != nil {
		Log("NewRmq err %s", err)
		return
	}
	err = r.Connect()
	if err != nil {
		Log("Connect err %s", err)
		return
	}
	// path := "direct/ex_dlx_direct/delay_direct"
	// queue := "delay_direct"
	// path := "fanout/ex_dlx_fanout/delay_fanout"
	// queue := "delay_fanout"
	path := "topic/ex_dlx_topic/delay_topic"
	queue := "delay_topic"

	replyTo := "pig"
	timeDelay := time.Millisecond * 100
	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)
	msgs, _, err := r.GenConsumerDlx(path, queue, "")
	if err != nil {
		Log("GenConsumer err %s", err)
		return
	}

	go func() {
		i := 0
		for {
			select {
			case <-ctx.Done():
				Log("publish_dlx ctx done %d", i)
				return
			case <-time.After(timeDelay):
				i++
				id := fmt.Sprintf("%d", i)
				err := r.PublishDlx(ctx, path, id, replyTo, "dlx_msg", time.Second*2)
				if err != nil {
					break
				} else {
					Log("publish_dlx succ %d", i)
				}
				if i > 20 {
					Log("publish_dlx over %d", i)
					return
				}
			}
		}
	}()

	err = r.Receive(ctx, msgs, func(msg *amqp.Delivery, err error) (ackOrReject bool, stop bool) {
		if err != nil {
			Log("receive dlx_msg async error %s", err)
		} else {
			id := msg.CorrelationId
			Log("receive dlx_msg async %s : %s %s\n", id, msg.ReplyTo, string(msg.Body))
		}
		return true, false
	})
	if err != nil {
		Log("receive dlx_msg err %s", err)
	}
	Log("test dlx_msg over")
}

func main() {
	// test()
	testDlx()
}
