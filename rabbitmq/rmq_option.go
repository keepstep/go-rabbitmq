package rabbitmq

import (
	"time"

	clone "github.com/huandu/go-clone/generic"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Option func(*RmqOption)

type RmqOption struct {
	//queue and exchange
	Durable    bool
	AutoDelete bool
	NoWait     bool
	Exclusive  bool
	QueueArg   amqp.Table
	MessageTTL int //merge into QueueArg, can be reset by OptionQueueArg. eg:"x-mesage-ttl":86400000
	Expires    int //merge into QueueArg, can be reset by OptionQueueArg. eg:"x-expires":86400000

	//exchange
	ExchangeInternal bool
	ExchangeArg      amqp.Table

	//publish
	Mandatory       bool
	Immediate       bool
	DeliveryMode    uint8
	ContentType     string
	ContentEncoding string
	MessageType     string

	//consume
	ConsumeAutoAck   bool
	ConsumeExclusive bool //do not effect on rpc and stream ,always true
	ConsumeNoWait    bool
	ConsumeArg       amqp.Table

	//Qos
	QosPrefetchCount int
	QosPrefetchSize  int
	QosGlobal        bool

	//rpc
	RpcPurgePubQueue   bool
	RpcPurgeReplyQueue bool
	//stream
	StreamPurgePubQueue   bool
	StreamPurgeReplyQueue bool
}

/*
amqp.Table{
	"x-mesage-ttl":86400000, //队列中消息的过期时间，单位为毫秒, 和消息pub时的过期时间取最小值起作用
	"x-expires":86400000, //表示超过该设定的时间，队列将被自动删除,
	// "x-dead-letter-exchange": exchange,
	// "x-dead-letter-routing-key":queue, //死信交换机根据当前重新指定的routin key将消息重新路由到死信队列
	// "x-max-length",表示队列中能存储的最大消息数，超过该数值后，消息变为死信
}
*/

func NewOption(args ...Option) RmqOption {
	opt := RmqOption{
		Durable:          true,
		DeliveryMode:     amqp.Persistent,
		ContentType:      "plain/text",
		QueueArg:         nil,
		ExchangeArg:      nil,
		ConsumeArg:       nil,
		QosPrefetchCount: 1,
		QosPrefetchSize:  0,
		QosGlobal:        false,
	}
	for _, f := range args {
		if f != nil {
			f(&opt)
		}
	}
	return opt
}

func ApplyOption(opt *RmqOption, args ...Option) {
	for _, f := range args {
		if f != nil {
			f(opt)
		}
	}
}

func CloneOption(opt *RmqOption) *RmqOption {
	return clone.Clone(opt)
}

func OptionDurable(durable bool) Option {
	return func(opt *RmqOption) {
		opt.Durable = durable
	}
}

func OptionAutoDelete(autoDelete bool) Option {
	return func(opt *RmqOption) {
		opt.AutoDelete = autoDelete
	}
}

func OptionNoWait(noWait bool) Option {
	return func(opt *RmqOption) {
		opt.NoWait = noWait
	}
}

func OptionExclusive(exclusive bool) Option {
	return func(opt *RmqOption) {
		opt.Exclusive = exclusive
	}
}

func OptionQueueArg(arg amqp.Table) Option {
	return func(opt *RmqOption) {
		opt.QueueArg = arg
	}
}

func OptionMessageTTL(t time.Duration) Option {
	return func(opt *RmqOption) {
		opt.MessageTTL = int(t.Milliseconds())
		if opt.QueueArg == nil {
			opt.QueueArg = amqp.Table{}
		}
		opt.QueueArg["x-mesage-ttl"] = opt.MessageTTL
	}
}

func OptionExpires(t time.Duration) Option {
	return func(opt *RmqOption) {
		opt.Expires = int(t.Milliseconds())
		if opt.QueueArg == nil {
			opt.QueueArg = amqp.Table{}
		}
		opt.QueueArg["x-expires"] = opt.Expires
	}
}

func OptionExchangeInternal(internal bool) Option {
	return func(opt *RmqOption) {
		opt.ExchangeInternal = internal
	}
}

func OptionExchangeArg(arg amqp.Table) Option {
	return func(opt *RmqOption) {
		opt.ExchangeArg = arg
	}
}

func OptionMandatory(mandatory bool) Option {
	return func(opt *RmqOption) {
		opt.Mandatory = mandatory
	}
}

func OptionImmediate(immediate bool) Option {
	return func(opt *RmqOption) {
		opt.Immediate = immediate
	}
}

func OptionDeliveryMode(deliveryMode uint8) Option {
	return func(opt *RmqOption) {
		if deliveryMode == amqp.Persistent || deliveryMode == amqp.Transient {
			opt.DeliveryMode = deliveryMode
		}
	}
}

func OptionContentType(contentType string) Option {
	return func(opt *RmqOption) {
		opt.ContentType = contentType
	}
}

func OptionContentEncoding(contentEncoding string) Option {
	return func(opt *RmqOption) {
		opt.ContentEncoding = contentEncoding
	}
}

func OptionMessageType(messageType string) Option {
	return func(opt *RmqOption) {
		opt.MessageType = messageType
	}
}

func OptionConsumeAutoAck(autoAck bool) Option {
	return func(opt *RmqOption) {
		opt.ConsumeAutoAck = autoAck
	}
}

func OptionConsumeExclusive(exclusive bool) Option {
	return func(opt *RmqOption) {
		opt.ConsumeExclusive = exclusive
	}
}

func OptionConsumeNoWait(noWait bool) Option {
	return func(opt *RmqOption) {
		opt.ConsumeNoWait = noWait
	}
}

func OptionConsumeArg(arg amqp.Table) Option {
	return func(opt *RmqOption) {
		opt.ConsumeArg = arg
	}
}

func OptionQos(prefetchCount, prefetchSize int, global bool) Option {
	return func(opt *RmqOption) {
		opt.QosPrefetchCount = prefetchCount
		opt.QosPrefetchSize = prefetchSize
		opt.QosGlobal = global
	}
}

func OptionRpcPurgeQueue(purgePub, purgeReply bool) Option {
	return func(opt *RmqOption) {
		opt.RpcPurgePubQueue = purgePub
		opt.RpcPurgeReplyQueue = purgeReply
	}
}

func OptionStreamPurgeQueue(purgePub, purgeReply bool) Option {
	return func(opt *RmqOption) {
		opt.StreamPurgePubQueue = purgePub
		opt.StreamPurgeReplyQueue = purgeReply
	}
}
