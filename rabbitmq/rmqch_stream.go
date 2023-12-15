package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RmqStreamData struct {
	Index         int
	CorrelationId string
	ReqBody       []byte
	RspBody       []byte
}

func (r *RmqCh) Stream(ctx context.Context, queue, replyTo string, streamFunc func(isReq bool, data *RmqStreamData) (stop bool)) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	if streamFunc == nil {
		err = errors.New("stream error func nil")
		return
	}
	if queue == "" {
		err = errors.New("stream error queue empty")
		return
	}
	if replyTo == "" {
		err = errors.New("stream error replyto empty")
		return
	}
	_, err = r.ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return
	}
	_, err = r.ch.QueueDeclare(replyTo, true, false, false, false, nil)
	if err != nil {
		return
	}

	i := 0
	//must cancel stream for use only one consume
	tag := fmt.Sprintf("stream_%p", r.ch)
	r.ch.Cancel(tag, false)
	msgs, eee := r.ch.Consume(
		replyTo, // queue
		tag,     // consumer
		false,   // auto ack
		true,    // exclusive
		false,   // no local
		false,   // no wait
		nil,     // args
	)
	err = eee
	if err != nil {
		amqp.Logger.Printf("stream error consume:%s", err)
		return
	}

	for {
		data := new(RmqStreamData)
		data.Index = i
		stop := streamFunc(true, data)
		data.Index = i
		if stop {
			return
		}
		if data.CorrelationId == "" {
			err = errors.New("stream correlationId empty")
			return
		}
		if r.IsClosed() {
			err = errors.New("stream channel closed")
			return
		}
		correlationId := data.CorrelationId
		err = r.ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			DeliveryMode:  amqp.Persistent,
			ContentType:   "text/plain",
			ReplyTo:       replyTo,
			CorrelationId: data.CorrelationId,
			Body:          data.ReqBody,
		})
		if err != nil {
			amqp.Logger.Printf("stream error publish: %d %s", i, err)
		} else {
			// amqp.Logger.Printf("rmq publish succ :%s %s", exchange, key)
		}

		stopLoop := false
		for {
			select {
			case msg, ok := <-msgs:
				if !ok {
					err = errors.New("stream receive msgs close")
					return
				}
				if msg.CorrelationId == correlationId {
					msg.Ack(false)
					amqp.Logger.Printf("stream receive ok %s", correlationId)
					data.RspBody = msg.Body
					stop := streamFunc(false, data)
					if stop {
						return
					}
					stopLoop = true
				} else {
					amqp.Logger.Printf("stream receive ignore continue %s", msg.CorrelationId)
					// msg.Reject(true)
					msg.Ack(false)
				}
			case <-r.done:
				err = errors.New(r.lastError)
				amqp.Logger.Printf("stream receive error done:%s", err)
				return
			case <-ctx.Done():
				err = errors.New(RmqCtxDone)
				amqp.Logger.Printf("stream receive error ctx_down:%s", err)
				return
			}

			if stopLoop {
				break
			}
		}
		i++
	}
}
