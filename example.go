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
	msgs, _, err := r.Consume(path, queue, "")
	if err != nil {
		Log("Consume err %s", err)
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
				err := r.Publish(ctx, path, id, replyTo, []byte("123"))
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
	r, err := rmq.NewRmq(urls, nil,
		rmq.OptionAutoDelete(true),
		// rmq.OptionExpires(time.Second*10),
	)
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
	r.QueueDelete(queue, false, false)
	r.QueueDelete(queue+"_dlx", false, false)
	r.QueueDelete(replyTo, false, false)
	r.ExchangeDelete("ex_dlx_topic", false)
	msgs, _, err := r.ConsumeDlx(path, queue, "")
	if err != nil {
		Log("Consume err %s", err)
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
				err := r.PublishDlx(ctx, path, id, replyTo, []byte("dlx_msg"), time.Second*2)
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
	<-time.After(time.Second * 5)
	Log("test dlx_msg over")
}

func testDlxMultiChannel() {
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
	rch, err := r.Channel("dlx")
	if err != nil {
		Log("Channel err %s", err)
		return
	}
	r.QueueDelete(queue, false, false)
	r.QueueDelete(queue+"_dlx", false, false)
	r.QueueDelete(replyTo, false, false)
	r.ExchangeDelete("ex_dlx_topic", false)
	msgs, _, err := rch.ConsumeDlx(path, queue, "")
	if err != nil {
		Log("Consume err %s", err)
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
				err := r.PublishDlx(ctx, path, id, replyTo, []byte("dlx_msg"), time.Second*2)
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

	err = rch.Receive(ctx, msgs, func(msg *amqp.Delivery, err error) (ackOrReject bool, stop bool) {
		if err != nil {
			Log("receive dlx_msg async error %s", err)
		} else {
			id := msg.CorrelationId
			Log("receive dlx_msg async %s : %s %s\n", id, msg.ReplyTo, string(msg.Body))
		}
		<-time.After(time.Second)
		return true, false
	})
	if err != nil {
		Log("receive dlx_msg err %s", err)
	}
	Log("test dlx_msg over")
}

func testReconnect() {
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
	path := "direct/ex_direct/ree_q"
	queue := "ree_q"

	replyTo := "pig"
	timeDelay := time.Millisecond * 1000
	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)
	go func() {
		i := 1
		for {
			select {
			case <-ctx.Done():
				Log("publish_reconnect ctx done %d", i)
				return
			case <-time.After(timeDelay):
				id := fmt.Sprintf("%d", i)
				err := r.Publish(ctx, path, id, replyTo, []byte("recon_msg"))
				if err != nil {
					Log("publish_reconnect err %d %s", i, err)
					<-time.After(time.Second * 2)
				} else {
					Log("publish_reconnect succ %d", i)
					i++
				}
			}
		}
	}()
	for {
		rch, err := r.Channel("pig")
		if err != nil {
			Log("receive Channel err %s", err)
			<-time.After(time.Second * 2)
			continue
		}
		msgs, _, err := rch.Consume(path, queue, "")
		if err != nil {
			Log("receive Consume err %s", err)
			<-time.After(time.Second * 2)
			continue
		}
		err = rch.Receive(ctx, msgs, func(msg *amqp.Delivery, err error) (ackOrReject bool, stop bool) {
			if err != nil {
				Log("receive error %s", err)
			} else {
				id := msg.CorrelationId
				Log("receive %s : %s %s\n", id, msg.ReplyTo, string(msg.Body))
			}
			return true, false
		})
		if err != nil {
			Log("receive err %s", err)
		}
		if ctx.Err() != nil {
			Log("receive over")
			break
		}
	}

	Log("test reconnect over")
}

func testRpc() {
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
	queue := "rpc_q"
	replyTo := "rpc_q_return"
	timeDelay := time.Millisecond * 1000
	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)
	go func() {
		i := 1
		for {
			select {
			case <-ctx.Done():
				Log("rpc ctx done %d", i)
				return
			default:
				id := fmt.Sprintf("cid%03d", i)
				// Log("rpc start %d", i)
				bs, err := r.Rpc(ctx, queue, id, replyTo, []byte("msg"))
				if err != nil {
					Log("rpc err %d %s", i, err)
					<-time.After(time.Second * 2)
				} else {
					Log("rpc end %s %s", id, string(bs))
					i++
				}
				<-time.After(timeDelay)
			}
		}
	}()
	for {
		rch, err := r.Channel("pig")
		if err != nil {
			Log("rpc Channel err %s", err)
			<-time.After(time.Second * 2)
			continue
		}
		msgs, _, err := rch.Consume(queue, queue, "")
		if err != nil {
			Log("rpc Consume err %s", err)
			<-time.After(time.Second * 2)
			continue
		}
		err = rch.Receive(ctx, msgs, func(msg *amqp.Delivery, err error) (ackOrReject bool, stop bool) {
			if err != nil {
				Log("   receive error %s", err)
			} else {
				id := msg.CorrelationId
				Log("   receive %s : %s %s\n", id, msg.ReplyTo, string(msg.Body))
				err = rch.Publish(ctx, msg.ReplyTo, id, "", []byte(fmt.Sprintf("%s_%s", string(msg.Body), id)))
				if err != nil {
					Log("   receive return error %s", err)
				} else {
					Log("   receive return succ %s", id)
				}
			}
			return true, false
		})
		if err != nil {
			Log("receive err %s", err)
		}
		if ctx.Err() != nil {
			Log("receive over")
			break
		}
	}

	Log("test rpc over")
}

func testStream() {
	urls := []string{
		"amqp://keep:keep@localhost:7672/aaaa",
		"amqp://keep:keep@localhost:8672/aaaa",
		"amqp://keep:keep@localhost:6672/aaaa",
	}
	r, err := rmq.NewRmq(urls, nil,
		rmq.OptionAutoDelete(true),
		rmq.OptionContentType("json"),
		rmq.OptionStreamPurgeQueue(true, true),
	)
	if err != nil {
		Log("NewRmq err %s", err)
		return
	}
	err = r.Connect()
	if err != nil {
		Log("Connect err %s", err)
		return
	}
	queue := "stream_q"
	replyTo := "stream_q_return"
	count := 35
	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)
	// r.QueueDelete(queue, false, false)
	// r.QueueDelete(replyTo, false, false)
	q, eeee := r.QueueInspect(queue)
	Log("stream inspect queue %s : %d %d %s", q.Name, q.Messages, q.Consumers, eeee)
	q, eeee = r.QueueInspect(replyTo)
	Log("stream inspect queue %s : %d %d %s", q.Name, q.Messages, q.Consumers, eeee)

	Log("--------------------------")
	Log("--------------------------")
	Log("--------------------------")

	go func() {
		eee := r.Stream(ctx, true, queue, replyTo, func(received bool, data *rmq.RmqStreamData) (stop bool) {
			if received {
				Log("stream active recv %s", string(data.RecvBody))
				Log("--------------------------")
				<-time.After(time.Second)
			} else {
				Log("stream active first %d", data.Index)
			}
			if data.Index >= count {
				return true
			} else {
				data.SendCorrelationId = fmt.Sprintf("sid_%03d", data.Index)
				data.SendBody = []byte("active_msg_" + data.SendCorrelationId)
				Log("stream active send %d %s", data.Index, string(data.SendBody))
				return false
			}
		})
		if eee != nil {
			Log("stream active err %s", eee)
		}
	}()
	// go func() {
	// 	<-time.After(time.Second * 5)
	// 	q, eeee := r.QueueInspect(queue)
	// 	if eeee == nil {
	// 		Log("stream inspect queue %s : %d %d", q.Name, q.Messages, q.Consumers)
	// 	}
	// 	q, eeee = r.QueueInspect(replyTo)
	// 	if eeee == nil {
	// 		Log("stream inspect queue %s : %d %d", q.Name, q.Messages, q.Consumers)
	// 	}
	// }()
	passiveFunc := func(name string) {
		rch, err := r.Channel(name)
		if err != nil {
			Log("stream Channel err %s", err)
		}
		eee := rch.Stream(ctx, false, "", queue, func(received bool, data *rmq.RmqStreamData) (stop bool) {
			if received {
				Log("stream passive recv %s", string(data.RecvBody))
				// <-time.After(time.Second)
			}
			if data.Index >= count {
				return true
			} else {
				data.SendCorrelationId = data.RecvCorrelationId
				data.SendBody = []byte(name + "_msg_" + data.RecvCorrelationId)
				return false
			}
		})
		if eee != nil {
			Log("stream passive err %s", eee)
		}
	}
	go passiveFunc("pig")
	go passiveFunc("dog")
	go passiveFunc("cat")
	go passiveFunc("bob")

	<-time.After(time.Second * 60)
	Log("test stream over")
}
