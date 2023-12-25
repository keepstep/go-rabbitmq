package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func (r *RmqCh) QueuePurge(queue string) (count int, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	count, err = r.ch.QueuePurge(queue, false)
	return
}

func (r *RmqCh) QueueDelete(queue string, isUnused, isEmpty bool) (count int, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	count, err = r.ch.QueueDelete(queue, isUnused, isEmpty, false)
	return
}

func (r *RmqCh) QueueUnbind(queue, key, exchange string) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	err = r.ch.QueueUnbind(queue, key, exchange, nil)
	return
}

func (r *RmqCh) ExchangeDelete(name string, isUnused bool) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	err = r.ch.ExchangeDelete(name, isUnused, false)
	return
}

func (r *RmqCh) ExchangeUnbind(destination, key, source string) (err error) {
	err = r.Check()
	if err != nil {
		return
	}
	err = r.ch.ExchangeUnbind(destination, key, source, false, nil)
	return
}

func (r *RmqCh) QueueInspect(name string) (q amqp.Queue, err error) {
	err = r.Check()
	if err != nil {
		return
	}
	q, err = r.ch.QueueDeclarePassive(name,
		r.option.Durable,
		r.option.AutoDelete,
		r.option.Exclusive,
		r.option.NoWait,
		r.option.QueueArg,
	)
	return
}
