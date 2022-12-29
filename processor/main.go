package processor

import (
	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/entities"
	"github.com/cjlapao/common-go/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

var logger = log.Get()

func ProcessMessage(d amqp.Delivery, handler entities.MessageHandler, options entities.ReceiverOptions) {
	logger.Info("Received message id %v for domain %v", d.CorrelationId, d.AppId)
	var msg adapters.Message
	var err error
	if msg, err = handler.Marshall(d.Body); err != nil {
		logger.Exception(err, "processing message")
	}

	result := handler.Handle(msg)
	if !result.Success() {
		logger.Exception(result.Error, "error handling message id %s", d.CorrelationId)

		if !options.AutoAck {
			logger.Info("Manually Acknowledging the message id %v", d.CorrelationId)
			d.Nack(result.Multiple, result.Requeue)
		}
	} else {
		if !options.AutoAck {
			logger.Info("Manually Acknowledging the message id %v", d.CorrelationId)
			d.Ack(false)
		}
		logger.Info("Finished processing message id %v", d.CorrelationId)
	}

}
