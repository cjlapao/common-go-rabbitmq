package entities

import (
	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/message"
)

type MessageHandler interface {
	GetType() string
	Handle(msg any) message.MessageResult
	GetMessage() adapters.Message
	Marshall(data []byte) (adapters.Message, error)
}

type QueueMessageHandler[T adapters.Message] struct {
	Message T
	Handler func(T) message.MessageResult
}

func GetQueueMessageHandlerType[T adapters.Message](x QueueMessageHandler[T]) string {
	return adapters.GetMessageLabel(x.Message)
}

func (s QueueMessageHandler[T]) GetType() string {
	return adapters.GetMessageLabel(s.Message)
}

func (s QueueMessageHandler[T]) Handle(msg any) message.MessageResult {
	return s.Handler(msg.(T))
}

func (s QueueMessageHandler[T]) GetMessage() adapters.Message {
	return s.Message
}

func (s QueueMessageHandler[T]) Marshall(data []byte) (adapters.Message, error) {
	value, err := adapters.MessageFromJson[T](data)

	return *value, err
}
