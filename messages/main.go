package messages

import (
	"encoding/json"

	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/entities"
	"github.com/cjlapao/common-go/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

var logger = log.Get()

func ProcessMessage[T adapters.Message](d amqp.Delivery, handler func(T), options entities.ReceiverOptions) {
	t := *new(T)
	messageType := adapters.GetMessageLabel(t)
	if d.AppId != messageType {
		logger.Warn("Received message of type %v and was expecting of type %v", d.AppId, messageType)
		if !options.AutoAck {
			logger.Info("Manually Acknowledging the message id %v", d.CorrelationId)
			d.Ack(false)
		}
	} else {
		logger.Info("Received message id %v for domain %v", d.CorrelationId, d.AppId)
		logger.Info(d.CorrelationId)
		var msg T
		err := json.Unmarshal(d.Body, &msg)
		if err != nil {
			logger.Exception(err, "processing message")
		}

		handler(msg)
		if !options.AutoAck {
			logger.Info("Manually Acknowledging the message id %v", d.CorrelationId)
			d.Ack(false)
		}

		logger.Info("Finished processing message id %v", d.CorrelationId)
	}
}
