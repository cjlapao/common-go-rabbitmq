package exchange_receiver

import (
	"errors"

	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/entities"
	"github.com/cjlapao/common-go-rabbitmq/messages"
	"github.com/cjlapao/common-go/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeReceiverService[T adapters.Message] struct {
	logger          *log.Logger
	connection      *amqp.Connection
	ServiceName     string
	ExchangeName    string
	Type            entities.ReceiverExchangeType
	Options         entities.ReceiverOptions
	ExchangeOptions entities.AmqpChannelOptions
	QueueOptions    entities.AmqpChannelOptions
	handler         func(T)
}

func New[T adapters.Message](connection *amqp.Connection) *ExchangeReceiverService[T] {
	result := ExchangeReceiverService[T]{
		logger:     log.Get(),
		connection: connection,
		Options: entities.ReceiverOptions{
			AutoAck:   true,
			Exclusive: false,
			NoLocal:   false,
			NoWait:    false,
		},
		Type: entities.Fanout,
		ExchangeOptions: entities.AmqpChannelOptions{
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
		},
		QueueOptions: entities.AmqpChannelOptions{
			Durable:    false,
			Exclusive:  true,
			AutoDelete: false,
			NoWait:     true,
		},
	}

	return &result
}

func (r *ExchangeReceiverService[T]) Name() string {
	t := *new(T)
	return adapters.GetMessageLabel(t)
}

func (r *ExchangeReceiverService[T]) WithOptions(options entities.ReceiverOptions) *ExchangeReceiverService[T] {
	r.Options = options
	return r
}

func (r *ExchangeReceiverService[T]) ManualAcknowledge() *ExchangeReceiverService[T] {
	r.Options.AutoAck = false
	return r
}

func (r *ExchangeReceiverService[T]) Exclusive() *ExchangeReceiverService[T] {
	r.Options.Exclusive = true
	return r
}

func (r *ExchangeReceiverService[T]) NoWait() *ExchangeReceiverService[T] {
	r.Options.Exclusive = true
	return r
}

func (r *ExchangeReceiverService[T]) HandleMessage(exchangeName string, h func(T)) {
	r.handler = h
	r.handle(exchangeName)
}

func (r *ExchangeReceiverService[T]) handle(exchangeName string) error {
	if r.handler == nil {
		return errors.New("no handler registered")
	}

	ch, err := r.connection.Channel()
	if err != nil {
		r.logger.Exception(err, "failed to create channel")
		return err
	}

	t := *new(T)
	messageType := adapters.GetMessageLabel(t)
	r.logger.Info("Opened RabbitMQ server channel for exchange %v and message %v", exchangeName, messageType)

	if err := ch.ExchangeDeclare(
		exchangeName,
		r.Type.String(),
		r.ExchangeOptions.Durable,
		r.ExchangeOptions.AutoDelete,
		r.ExchangeOptions.Internal,
		r.ExchangeOptions.NoWait,
		nil,
	); err != nil {
		return err
	}

	// Creating the Queue if it does not exist
	if ch.IsClosed() {
		ch, err = r.connection.Channel()
		if err != nil {
			r.logger.Exception(err, "failed to create channel")
			return err
		}
	}

	q, err := ch.QueueDeclare(
		"",
		r.QueueOptions.Durable,
		r.QueueOptions.AutoDelete,
		r.QueueOptions.Exclusive,
		r.QueueOptions.NoWait,
		nil,
	)

	if err != nil {
		r.logger.Exception(err, "failed to create exchange queue")
		return err
	}

	if ch.IsClosed() {
		ch, err = r.connection.Channel()
		if err != nil {
			r.logger.Exception(err, "failed to create channel")
			return err
		}
	}

	err = ch.QueueBind(
		q.Name,
		"",
		exchangeName,
		r.Options.NoWait,
		nil,
	)

	if err != nil {
		r.logger.Exception(err, "failed to bind to queue")
		return err
	}

	if ch.IsClosed() {
		ch, err = r.connection.Channel()
		if err != nil {
			r.logger.Exception(err, "failed to create channel")
			return err
		}
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		r.Options.AutoAck,
		r.Options.Exclusive,
		r.Options.NoLocal,
		r.Options.NoWait,
		nil,
	)
	if err != nil {
		r.logger.Exception(err, "failed to consume messages")
		return err
	}

	var forever chan struct{}

	r.logger.Info("Starting to handle messages %v for exchange %v", messageType, exchangeName)

	go func() {
		for d := range msgs {
			messages.ProcessMessage(d, r.handler, r.Options)
		}
	}()

	<-forever

	ch.Close()
	return nil
}
