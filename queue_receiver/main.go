package queue_receiver

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/client"
	"github.com/cjlapao/common-go-rabbitmq/entities"
	"github.com/cjlapao/common-go-rabbitmq/message"
	"github.com/cjlapao/common-go-rabbitmq/processor"
	"github.com/cjlapao/common-go/log"
	"github.com/rabbitmq/amqp091-go"
)

var registeredReceivers []*QueueReceiverService

type QueueReceiverService struct {
	logger        *log.Logger
	client        *client.RabbitMQClient
	QueueName     string
	Options       entities.ReceiverOptions
	QueueOptions  entities.AmqpQueueOptions
	PrefetchCount int
	handlers      []entities.MessageHandler
}

func StartHandling() {
	if registeredReceivers == nil {
		return
	}

	for _, handler := range registeredReceivers {
		go handler.Handle()
	}
}

func RegisterHandler[T adapters.Message](queueName string, h func(T) message.MessageResult) error {
	if registeredReceivers == nil {
		registeredReceivers = make([]*QueueReceiverService, 0)
	}

	var queueReceiver *QueueReceiverService
	for _, receiver := range registeredReceivers {
		if strings.EqualFold(receiver.QueueName, queueName) {
			queueReceiver = receiver
			break
		}
	}

	if queueReceiver == nil {
		queueReceiver = New()
		queueReceiver.QueueName = queueName
		if err := queueReceiver.createQueueIfNotExist(); err != nil {
			return err
		}

		registeredReceivers = append(registeredReceivers, queueReceiver)
	}

	t := *new(T)
	handler := entities.QueueMessageHandler[T]{
		Message: t,
		Handler: h,
	}

	queueReceiver.handlers = append(queueReceiver.handlers, handler)

	return nil
}

func New() *QueueReceiverService {
	result := QueueReceiverService{
		logger:        log.Get(),
		client:        client.Get(),
		PrefetchCount: 1,
		Options: entities.ReceiverOptions{
			AutoAck:   true,
			Exclusive: false,
			NoLocal:   false,
			NoWait:    false,
		},
		QueueOptions: entities.AmqpQueueOptions{
			Durable:    false,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
		},
	}
	result.handlers = make([]entities.MessageHandler, 0)

	return &result
}

func (r *QueueReceiverService) WithOptions(options entities.ReceiverOptions) *QueueReceiverService {
	r.Options = options
	return r
}

func (r *QueueReceiverService) ManualAcknowledge() *QueueReceiverService {
	r.Options.AutoAck = false
	return r
}

func (r *QueueReceiverService) Exclusive() *QueueReceiverService {
	r.Options.Exclusive = true
	return r
}

func (r *QueueReceiverService) NoWait() *QueueReceiverService {
	r.Options.Exclusive = true
	return r
}

func (r *QueueReceiverService) Handle() error {
	if r.handlers == nil || len(r.handlers) == 0 {
		return errors.New("no handler registered")
	}

	ch, err := r.client.GetChannel()
	if err != nil {
		r.logger.Exception(err, "failed to create channel")
		return err
	}

	r.logger.Info("Opened RabbitMQ server channel for queue %v", r.QueueName)

	defer ch.Close()

	ch.Qos(r.PrefetchCount, 0, false)
	msgs, err := ch.Consume(
		r.QueueName,
		fmt.Sprintf("Queue %s consumer handler", r.QueueName),
		r.Options.AutoAck,
		r.Options.Exclusive,
		r.Options.NoLocal,
		r.Options.NoWait,
		nil,
	)
	if err != nil {
		return err
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		for d := range msgs {
			processed := false
			for _, handler := range r.handlers {
				msgType := handler.GetType()
				if d.Type == msgType {
					r.logger.Info("Processing message %s in queue %s", msgType, r.QueueName)
					processor.ProcessMessage(d, handler, r.Options)
					processed = true
				}
			}

			if !processed {
				r.logger.Error("No handler found for message %s in queue %s", d.Type, r.QueueName)
			}
		}
	}()

	<-c

	return nil
}

func (r *QueueReceiverService) createQueueIfNotExist() error {
	ch, err := r.client.GetChannel()
	if err != nil {
		r.logger.Exception(err, "failed to create channel")
		return err
	}
	// Creating the Queue if it does not exist
	_, err = ch.QueueInspect(r.QueueName)
	if err != nil {
		r.logger.Info("Queue %v does not exists, creating it", r.QueueName)
		if ch.IsClosed() {
			ch, err = r.client.GetChannel()
			if err != nil {
				return err
			}
		}

		args := amqp091.Table{}
		args["x-queue-version"] = 2

		if _, err := ch.QueueDeclare(
			r.QueueName,
			r.QueueOptions.Durable,
			r.QueueOptions.AutoDelete,
			r.QueueOptions.Internal,
			r.QueueOptions.NoWait,
			args,
		); err != nil {
			r.logger.Exception(err, "creating queue %v", r.QueueName)
			return err
		}
	}

	return nil
}
