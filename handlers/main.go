package handlers

import (
	"strings"

	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/client"
	"github.com/cjlapao/common-go-rabbitmq/entities"
	"github.com/cjlapao/common-go-rabbitmq/exchange_receiver"
	"github.com/cjlapao/common-go-rabbitmq/message"
	"github.com/cjlapao/common-go-rabbitmq/queue_receiver"
	"github.com/cjlapao/common-go/log"
)

var logger = log.Get()

func RegisterQueueHandler[T adapters.Message](queueName string, handler func(T) message.MessageResult) {
	rmqClient := client.Get()

	t := *new(T)
	name := queueName + "." + adapters.GetMessageLabel(t)
	for _, queueHandlerName := range rmqClient.QueuesHandlers {
		if strings.EqualFold(queueHandlerName, name) {
			logger.Warn("There is already a handler for the queue %v and message type %v", queueName, adapters.GetMessageLabel(t))
			return
		}
	}

	handlerSvc := queue_receiver.New[T]()
	go handlerSvc.HandleMessage(queueName, handler)
	rmqClient.QueuesHandlers = append(rmqClient.QueuesHandlers, name)
}

func RegisterTopicExchangeHandler[T adapters.Message](exchangeName string, routingKey string, handler func(T) message.MessageResult) {
	rqmClient := client.Get()

	t := *new(T)
	name := exchangeName + "." + adapters.GetMessageLabel(t)
	for _, exchangeHandlerName := range rqmClient.ExchangeHandlers {
		if strings.EqualFold(exchangeHandlerName, name) {
			logger.Warn("There is already a handler for the exchange %v and message type %v", exchangeName, adapters.GetMessageLabel(t))
			return
		}
	}

	handlerSvc := exchange_receiver.New[T]()
	handlerSvc.Type = entities.Topic
	handlerSvc.RoutingKey = routingKey
	go handlerSvc.HandleMessage(exchangeName, handler)
	rqmClient.ExchangeHandlers = append(rqmClient.ExchangeHandlers, name)
}

func RegisterDirectExchangeHandler[T adapters.Message](exchangeName string, routingKey string, handler func(T) message.MessageResult) {
	rmqClient := client.Get()

	t := *new(T)
	name := exchangeName + "." + adapters.GetMessageLabel(t)
	for _, exchangeHandlerName := range rmqClient.ExchangeHandlers {
		if strings.EqualFold(exchangeHandlerName, name) {
			logger.Warn("There is already a handler for the exchange %v and message type %v", exchangeName, adapters.GetMessageLabel(t))
			return
		}
	}

	handlerSvc := exchange_receiver.New[T]()
	handlerSvc.Type = entities.Direct
	handlerSvc.RoutingKey = routingKey
	go handlerSvc.HandleMessage(exchangeName, handler)
	rmqClient.ExchangeHandlers = append(rmqClient.ExchangeHandlers, name)
}

func RegisterFanoutExchangeHandler[T adapters.Message](exchangeName string, handler func(T) message.MessageResult) {
	rmqClient := client.Get()

	t := *new(T)
	name := exchangeName + "." + adapters.GetMessageLabel(t)
	for _, exchangeHandlerName := range rmqClient.ExchangeHandlers {
		if strings.EqualFold(exchangeHandlerName, name) {
			logger.Warn("There is already a handler for the exchange %v and message type %v", exchangeName, adapters.GetMessageLabel(t))
			return
		}
	}

	handlerSvc := exchange_receiver.New[T]()
	go handlerSvc.HandleMessage(exchangeName, handler)
	rmqClient.ExchangeHandlers = append(rmqClient.ExchangeHandlers, name)
}

func RegisterExchangeHandler[T adapters.Message](exchangeName string, exchangeType entities.ReceiverExchangeType, routingKey string, handler func(T) message.MessageResult) {
	rmqClient := client.Get()

	t := *new(T)
	name := exchangeName + "." + adapters.GetMessageLabel(t)
	for _, exchangeHandlerName := range rmqClient.ExchangeHandlers {
		if strings.EqualFold(exchangeHandlerName, name) {
			logger.Warn("There is already a handler for the exchange %v and message type %v", exchangeName, adapters.GetMessageLabel(t))
			return
		}
	}

	handlerSvc := exchange_receiver.New[T]()
	go handlerSvc.HandleMessage(exchangeName, handler)
	rmqClient.ExchangeHandlers = append(rmqClient.ExchangeHandlers, name)
}
