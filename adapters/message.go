package adapters

import (
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Message interface {
	CorrelationID() string
	ContentType() string
	Body() []byte
	Name() string
	Version() string
	Domain() string
}

type MessageDeliveryMode int64

const (
	PersistentMessage MessageDeliveryMode = iota
	TransientMessage
)

func (t MessageDeliveryMode) ToAmqpDeliveryMode() uint8 {
	switch t {
	case PersistentMessage:
		return amqp.Persistent
	case TransientMessage:
		return amqp.Transient
	default:
		return amqp.Transient
	}
}

func GetMessageLabel[T Message](t T) string {
	return t.Domain() + "." + t.Name() + ".v" + strings.TrimPrefix(t.Version(), "v")
}
