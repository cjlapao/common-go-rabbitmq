package sender

import (
	"context"
	"fmt"
	"time"

	cryptorand "github.com/cjlapao/common-go-cryptorand"
	log "github.com/cjlapao/common-go-logger"
	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/client"
	"github.com/cjlapao/common-go-rabbitmq/entities"
	"github.com/cjlapao/common-go/constants"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SenderService struct {
	logger *log.LoggerService
	client *client.RabbitMQClient
}

func New() *SenderService {
	client := client.Get()

	result := SenderService{
		logger: log.Get(),
		client: client,
	}

	return &result
}

func (s *SenderService) Send(msg Message) error {
	ch, err := s.client.GetChannel()
	if err != nil {
		return err
	}

	if msg.Type == entities.QueueMessage {
		_, err := ch.QueueInspect(msg.Name)
		if err != nil {
			return err
		}
		// switching to
		msg.RoutingKey = msg.Name
		msg.Name = ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.client.DefaultTimeout)*time.Second)

	defer cancel()

	if ch.IsClosed() {
		ch, err = s.client.GetChannel()
		if err != nil {
			return err
		}
	}

	msgType := adapters.GetMessageLabel(msg.Message)
	mId := cryptorand.GetRandomString(constants.ID_SIZE)
	if err := ch.PublishWithContext(ctx, msg.Name, msg.RoutingKey, false, false, amqp.Publishing{
		DeliveryMode:  msg.DeliveryMode.ToAmqpDeliveryMode(),
		ContentType:   msg.Message.ContentType(),
		CorrelationId: msg.Message.CorrelationID(),
		MessageId:     mId,
		ReplyTo:       msg.CallbackQueue,
		Type:          msgType,
		Timestamp:     time.Now(),
		AppId:         msg.Message.Domain(),
		Body:          msg.Message.Body(),
		Headers: amqp.Table{
			"X-Domain":  msg.Message.Domain(),
			"X-Name":    msg.Message.Name(),
			"X-Version": msg.Message.Version(),
		},
	}); err != nil {
		return err
	}

	// Switching back to logs
	if msg.Type == entities.QueueMessage {
		msg.Name = msg.RoutingKey
		msg.RoutingKey = ""
	}

	logMsg := fmt.Sprintf("Message %s width id %s sent successfully to %s %s ", msgType, mId, msg.Name, msg.Type)
	if msg.RoutingKey != "" {
		logMsg += fmt.Sprintf(" with routing key %s", msg.RoutingKey)
	}
	if msg.CallbackQueue != "" {
		logMsg += fmt.Sprintf(" and calling back queue %s", msg.CallbackQueue)
	}

	s.logger.Info(logMsg)

	return nil
}
