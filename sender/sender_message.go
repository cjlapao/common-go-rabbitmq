package sender

import (
	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/entities"
)

type SenderMessage struct {
	Type          entities.SenderMessageType
	Name          string
	RoutingKey    string
	CallbackQueue string
	DeliveryMode  adapters.MessageDeliveryMode
	Message       adapters.Message
}

func NewQueueMessage(queueName string, msg adapters.Message) SenderMessage {
	return SenderMessage{
		Type:          entities.QueueMessage,
		Name:          queueName,
		RoutingKey:    "",
		CallbackQueue: "",
		DeliveryMode:  adapters.PersistentMessage,
		Message:       msg,
	}
}

func NewQueueMessageWithCallback(queueName string, callBackQueue string, msg adapters.Message) SenderMessage {
	return SenderMessage{
		Type:          entities.QueueMessage,
		Name:          queueName,
		RoutingKey:    "",
		CallbackQueue: callBackQueue,
		DeliveryMode:  adapters.PersistentMessage,
		Message:       msg,
	}
}

func NewTransientQueueMessage(queueName string, msg adapters.Message) SenderMessage {
	return SenderMessage{
		Type:          entities.QueueMessage,
		Name:          queueName,
		RoutingKey:    "",
		CallbackQueue: "",
		DeliveryMode:  adapters.TransientMessage,
		Message:       msg,
	}
}

func NewTransientQueueMessageWithCallback(queueName string, callBackQueue string, msg adapters.Message) SenderMessage {
	return SenderMessage{
		Type:          entities.QueueMessage,
		Name:          queueName,
		RoutingKey:    "",
		CallbackQueue: callBackQueue,
		DeliveryMode:  adapters.TransientMessage,
		Message:       msg,
	}
}

func NewExchangeMessage(queueName string, routingKey string, msg adapters.Message) SenderMessage {
	return SenderMessage{
		Type:          entities.ExchangeMessage,
		Name:          queueName,
		RoutingKey:    routingKey,
		CallbackQueue: "",
		DeliveryMode:  adapters.PersistentMessage,
		Message:       msg,
	}
}

func NewExchangeMessageWithCallback(queueName string, routingKey string, callBackQueue string, msg adapters.Message) SenderMessage {
	return SenderMessage{
		Type:          entities.ExchangeMessage,
		Name:          queueName,
		RoutingKey:    routingKey,
		CallbackQueue: callBackQueue,
		DeliveryMode:  adapters.PersistentMessage,
		Message:       msg,
	}
}

func NewTransientExchangeMessage(queueName string, routingKey string, msg adapters.Message) SenderMessage {
	return SenderMessage{
		Type:          entities.ExchangeMessage,
		Name:          queueName,
		RoutingKey:    routingKey,
		CallbackQueue: "",
		DeliveryMode:  adapters.TransientMessage,
		Message:       msg,
	}
}

func NewTransientExchangeMessageWithCallback(queueName string, routingKey string, callBackQueue string, msg adapters.Message) SenderMessage {
	return SenderMessage{
		Type:          entities.ExchangeMessage,
		Name:          queueName,
		RoutingKey:    routingKey,
		CallbackQueue: callBackQueue,
		DeliveryMode:  adapters.TransientMessage,
		Message:       msg,
	}
}
