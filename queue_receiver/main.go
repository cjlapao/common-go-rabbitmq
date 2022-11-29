package queue_receiver

import (
	"errors"

	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/entities"
	"github.com/cjlapao/common-go-rabbitmq/message"
	"github.com/cjlapao/common-go-rabbitmq/processor"
	"github.com/cjlapao/common-go/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueReceiverService[T adapters.Message] struct {
	logger       *log.Logger
	connection   *amqp.Connection
	channel      *amqp.Channel
	QueueName    string
	Options      entities.ReceiverOptions
	QueueOptions entities.AmqpChannelOptions
	handler      func(T) message.MessageResult
}

func New[T adapters.Message](connection *amqp.Connection) *QueueReceiverService[T] {
	result := QueueReceiverService[T]{
		logger:     log.Get(),
		connection: connection,
		Options: entities.ReceiverOptions{
			AutoAck:   true,
			Exclusive: false,
			NoLocal:   false,
			NoWait:    false,
		},
		QueueOptions: entities.AmqpChannelOptions{
			Durable:    false,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
		},
	}

	return &result
}

func (r *QueueReceiverService[T]) Name() string {
	t := *new(T)
	return r.QueueName + "." + adapters.GetMessageLabel(t)
}

func (r *QueueReceiverService[T]) WithOptions(options entities.ReceiverOptions) *QueueReceiverService[T] {
	r.Options = options
	return r
}

func (r *QueueReceiverService[T]) ManualAcknowledge() *QueueReceiverService[T] {
	r.Options.AutoAck = false
	return r
}

func (r *QueueReceiverService[T]) Exclusive() *QueueReceiverService[T] {
	r.Options.Exclusive = true
	return r
}

func (r *QueueReceiverService[T]) NoWait() *QueueReceiverService[T] {
	r.Options.Exclusive = true
	return r
}

func (r *QueueReceiverService[T]) HandleMessage(queueName string, h func(T) message.MessageResult) {
	r.handler = h
	r.handle(queueName)
}

func (r *QueueReceiverService[T]) handle(queueName string) error {
	if r.handler == nil {
		return errors.New("no handler registered")
	}

	r.QueueName = queueName
	if r.connection == nil || r.connection.IsClosed() {
		return errors.New("failed to connect to server or connection closed")
	}

	ch, err := r.connection.Channel()
	if err != nil {
		r.logger.Exception(err, "failed to create channel")
		return err
	}

	// Creating the Queue if it does not exist
	_, err = ch.QueueInspect(queueName)
	if err != nil {
		r.logger.Info("Queue %v does not exists, creating it", queueName)
		createChannel, err := r.connection.Channel()

		if err != nil {
			r.logger.Exception(err, "failed to create channel to create queue")
			return err
		}
		if _, err := createChannel.QueueDeclare(
			r.QueueName,
			r.QueueOptions.Durable,
			r.QueueOptions.AutoDelete,
			r.QueueOptions.Internal,
			r.QueueOptions.NoWait,
			nil,
		); err != nil {
			r.logger.Exception(err, "creating queue %v", queueName)
			createChannel.Close()
			return err
		}

		createChannel.Close()
	}

	ch.Close()

	ch, err = r.connection.Channel()
	if err != nil {
		r.logger.Exception(err, "failed to create channel")
		return err
	}

	t := *new(T)
	messageType := adapters.GetMessageLabel(t)
	r.logger.Info("Opened RabbitMQ server channel for queue %v and message %v", queueName, messageType)

	defer ch.Close()

	msgs, err := ch.Consume(
		r.QueueName,
		"",
		r.Options.AutoAck,
		r.Options.Exclusive,
		r.Options.NoLocal,
		r.Options.NoWait,
		nil,
	)
	if err != nil {
		return err
	}

	var forever chan struct{}

	r.logger.Info("starting to handle messages %v", messageType)
	go func() {
		for d := range msgs {
			processor.ProcessMessage(d, r.handler, r.Options)
		}
	}()

	<-forever

	return nil
}
