package sender

import (
	"context"
	"time"

	cryptorand "github.com/cjlapao/common-go-cryptorand"
	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/client"
	"github.com/cjlapao/common-go/constants"
	"github.com/cjlapao/common-go/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SenderService struct {
	logger       *log.Logger
	connection   *amqp.Connection
	exchangeName string
	queueName    string
	Name         string
}

func New() *SenderService {
	client := client.Get()

	result := SenderService{
		logger:     log.Get(),
		connection: client.Connection,
	}

	return &result
}

func (sender *SenderService) SendPersistentToQueue(queueName string, message adapters.Message) error {
	return sender.Send("", queueName, message, adapters.PersistentMessage)
}

func (sender *SenderService) SendTransientToQueue(queueName string, message adapters.Message) error {
	return sender.Send("", queueName, message, adapters.TransientMessage)
}

func (sender *SenderService) SendPersistentToExchange(exchangeName string, message adapters.Message) error {
	return sender.Send(exchangeName, "", message, adapters.PersistentMessage)
}

func (sender *SenderService) SendTransientToExchange(exchangeName string, message adapters.Message) error {
	return sender.Send(exchangeName, "", message, adapters.TransientMessage)
}

func (s *SenderService) Send(exchangeName string, queueName string, message adapters.Message, deliveryMode adapters.MessageDeliveryMode) error {
	s.exchangeName = exchangeName
	s.queueName = queueName

	ch, err := s.connection.Channel()
	if err != nil {
		s.logger.Exception(err, "failed to create channel")
		return err
	}

	if s.queueName != "" {
		_, err := ch.QueueInspect(queueName)
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	msgType := adapters.GetMessageLabel(message)
	mId := cryptorand.GetRandomString(constants.ID_SIZE)
	if err := ch.PublishWithContext(ctx, s.exchangeName, s.queueName, false, false, amqp.Publishing{
		DeliveryMode:  deliveryMode.ToAmqpDeliveryMode(),
		ContentType:   message.ContentType(),
		CorrelationId: message.CorrelationID(),
		MessageId:     mId,
		Type:          msgType,
		Timestamp:     time.Now(),
		AppId:         adapters.GetMessageLabel(message),
		Body:          message.Body(),
		Headers: amqp.Table{
			"X-Domain":  message.Domain(),
			"X-Name":    message.Name(),
			"X-Version": message.Version(),
		},
	}); err != nil {
		return err
	}

	if queueName != "" {
		s.logger.Info("Message %v width id %v sent successfully to queue %v", msgType, mId, queueName)
	} else {
		s.logger.Info("Message %v width id %v sent successfully to exchange %v", msgType, mId, exchangeName)
	}

	return nil
}
