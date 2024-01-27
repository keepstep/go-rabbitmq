package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RmqStreamData struct {
	Index int

	SendCorrelationId string
	SendBody          []byte

	RecvCorrelationId string
	RecvBody          []byte
}

// msg one by one
// sender(active=true):receiver(active=false) = 1:n
// if active = false: queue will not be declare(use msg.ReplayTo), consume not exclusive
// stepFunc: fill_send_data or handle_recv_data and fill_send_data
func (r *RmqCh) Stream(ctx context.Context, active bool, queue, replyTo string, stepFunc func(received bool, data *RmqStreamData) (stop bool)) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	if stepFunc == nil {
		err = errors.New("stream error func nil")
		return
	}
	if replyTo == "" {
		err = errors.New("stream error replyto empty")
		return
	}

	if active {
		if queue == "" {
			err = errors.New("stream error queue empty")
			return
		}
		_, err = r.ch.QueueDeclare(queue,
			r.option.Durable,
			r.option.AutoDelete,
			r.option.Exclusive,
			r.option.NoWait,
			r.option.QueueArg,
		)
		if err != nil {
			return
		}
	}

	_, err = r.ch.QueueDeclare(replyTo,
		r.option.Durable,
		r.option.AutoDelete,
		r.option.Exclusive,
		r.option.NoWait,
		r.option.QueueArg,
	)
	if err != nil {
		return
	}
	if active {
		if r.option.StreamPurgePubQueue {
			r.ch.QueuePurge(queue, false)
		}
	}
	if r.option.StreamPurgeReplyQueue {
		r.ch.QueuePurge(replyTo, false)
	}

	tag := ""
	consumeExclusive := true
	if active {
		//must cancel stream for use only one consume
		tag = fmt.Sprintf("stream_active_%p", r.ch)
		r.ch.Cancel(tag, false)
	} else {
		//not active means this is receiver,not exclusive
		tag = fmt.Sprintf("stream_passive_%p", r.ch)
		consumeExclusive = false
	}
	msgs, eee := r.ch.Consume(
		replyTo,                 // queue
		tag,                     // consumer
		r.option.ConsumeAutoAck, // auto ack
		consumeExclusive,        // exclusive
		false,                   // no local
		r.option.ConsumeNoWait,  // no wait
		r.option.ConsumeArg,     // args
	)
	err = eee
	if err != nil {
		amqp.Logger.Printf("stream error consume:%s", err)
		return
	}
	index := 0
	data := new(RmqStreamData)
	sendFunc := func(sendQueue string) (err error) {
		data.Index = index
		if len(data.SendBody) == 0 {
			err = errors.New("stream send body empty")
			return err
		}
		if r.IsClosed() {
			err = errors.New("stream send channel closed")
			return err
		}
		err = r.ch.PublishWithContext(ctx, "", sendQueue,
			r.option.Mandatory,
			r.option.Immediate,
			amqp.Publishing{
				DeliveryMode:    r.option.DeliveryMode,
				ContentType:     r.option.ContentType,
				ContentEncoding: r.option.ContentEncoding,
				Type:            r.option.MessageType,
				ReplyTo:         replyTo,
				CorrelationId:   data.SendCorrelationId,
				Body:            data.SendBody,
			})
		if err != nil {
			amqp.Logger.Printf("stream send error publish: %d %s", index, err)
			return err
		} else {
			// amqp.Logger.Printf("rmq publish succ :%s %s", exchange, key)
			return nil
		}
	}
	if active {
		stop := stepFunc(false, data)
		if stop {
			return
		}
		index++
		err = sendFunc(queue)
		if err != nil {
			return err
		}
	}

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				err = errors.New("stream recv msgs close")
				amqp.Logger.Printf("stream recv msgs close:%s", err)
				return
			}
			if active {
				if msg.CorrelationId != data.SendCorrelationId {
					amqp.Logger.Printf("stream recv ignore continue %s", msg.CorrelationId)
					// msg.Reject(true)
					msg.Ack(false)
					continue
				}
			}
			msg.Ack(false)
			// amqp.Logger.Printf("stream recv ok %s", msg.CorrelationId)
			data.RecvBody = msg.Body
			data.RecvCorrelationId = msg.CorrelationId
			data.SendBody = nil
			data.SendCorrelationId = ""
			stop := stepFunc(true, data)
			if stop {
				return
			}
			index++
			if data.SendBody != nil {
				if active {
					err = sendFunc(queue)
				} else {
					err = sendFunc(msg.ReplyTo)
				}
				if err != nil {
					return err
				}
			}
		case <-r.done:
			err = errors.New(r.lastError)
			amqp.Logger.Printf("stream recv error done:%s", err)
			return
		case <-ctx.Done():
			err = errors.New(RmqCtxDone)
			amqp.Logger.Printf("stream recv error ctx_done:%s", err)
			return
		}
	}
}
