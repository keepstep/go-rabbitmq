package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (r *RmqCh) Rpc(ctx context.Context, queue, correlationId, replyTo, body string) (rst []byte, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	amqp.Logger.Printf("rpc start: %s", correlationId)
	if queue == "" {
		amqp.Logger.Printf("rpc error queue empty")
		return
	}
	if replyTo == "" {
		amqp.Logger.Printf("rpc error replyto empty")
		return
	}
	if correlationId == "" {
		amqp.Logger.Printf("rpc error correlationId empty")
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
		amqp.Logger.Printf("rpc error queue declare")
		return
	}
	_, err = r.ch.QueueDeclare(replyTo,
		r.option.Durable,
		r.option.AutoDelete,
		r.option.Exclusive,
		r.option.NoWait,
		r.option.QueueArg,
	)
	if err != nil {
		amqp.Logger.Printf("rpc error replyTo declare")
		return
	}

	if r.option.RpcPurgePubQueue {
		r.ch.QueuePurge(queue, false)
	}
	if r.option.RpcPurgeReplyQueue {
		r.ch.QueuePurge(replyTo, false)
	}

	err = r.ch.PublishWithContext(ctx, "", queue,
		r.option.Mandatory,
		r.option.Immediate,
		amqp.Publishing{
			DeliveryMode:    r.option.DeliveryMode,
			ContentType:     r.option.ContentType,
			ContentEncoding: r.option.ContentEncoding,
			Type:            r.option.MessageType,
			ReplyTo:         replyTo,
			CorrelationId:   correlationId,
			Body:            []byte(body),
		})
	if err != nil {
		amqp.Logger.Printf("rpc error publish:%s", err)
	} else {
		// amqp.Logger.Printf("rmq publish succ :%s %s", exchange, key)
	}

	//must cancel rpc for use only one consume
	tag := fmt.Sprintf("rpc_%p", r.ch)
	r.ch.Cancel(tag, false)
	msgs, err := r.ch.Consume(
		replyTo,                 // queue
		tag,                     // consumer
		r.option.ConsumeAutoAck, // auto ack
		true,                    // exclusive
		false,                   // no local
		r.option.ConsumeNoWait,  // no wait
		r.option.ConsumeArg,     // args
	)
	if err != nil {
		amqp.Logger.Printf("rpc error consume:%s", err)
		return
	}
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				err = errors.New("rpc receive msgs close")
				return
			}
			if msg.CorrelationId == correlationId {
				msg.Ack(false)
				rst = msg.Body
				amqp.Logger.Printf("rpc receive ok %s", correlationId)
				return
			} else {
				amqp.Logger.Printf("rpc receive ignore continue %s", msg.CorrelationId)
				// msg.Reject(true)
				msg.Ack(false)
			}
		case <-r.done:
			err = errors.New(r.lastError)
			amqp.Logger.Printf("rpc receive error done:%s", err)
			return
		case <-ctx.Done():
			err = errors.New(RmqCtxDone)
			amqp.Logger.Printf("rpc receive error ctx_down:%s", err)
			return
		}
	}
}
