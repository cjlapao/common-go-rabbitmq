package client

import (
	"strings"

	"github.com/cjlapao/common-go-rabbitmq/adapters"
	"github.com/cjlapao/common-go-rabbitmq/constants"
	"github.com/cjlapao/common-go-rabbitmq/exchange_receiver"
	"github.com/cjlapao/common-go-rabbitmq/queue_receiver"
	"github.com/cjlapao/common-go/execution_context"
	"github.com/cjlapao/common-go/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

var globalRabbitMQClient *RabbitMQClient

type RabbitMQClient struct {
	logger           *log.Logger
	ConnectionString string
	Connection       *amqp.Connection
	ExchangeHandlers []string
	QueuesHandlers   []string
}

func New(ConnectionString string) *RabbitMQClient {

	client := RabbitMQClient{
		logger:           log.Get(),
		ConnectionString: ConnectionString,
		ExchangeHandlers: make([]string, 0),
		QueuesHandlers:   make([]string, 0),
	}

	if client.Connection == nil {
		conn, err := amqp.Dial(client.ConnectionString)
		if err != nil {
			client.logger.Exception(err, "connecting to rabbitmq server")
			return nil
		}

		client.Connection = conn
		client.logger.Info("Connected to RabbitMQ server")
	}

	return &client
}

func Get() *RabbitMQClient {
	if globalRabbitMQClient != nil {
		return globalRabbitMQClient
	}

	ctx := execution_context.Get()
	connString := ctx.Configuration.GetString(constants.RABBITMQ_CONNECTION_STRING_NAME)
	globalRabbitMQClient = New(connString)
	return globalRabbitMQClient
}

func RegisterQueueHandler[T adapters.Message](queueName string, handler func(T)) {
	client := Get()

	t := *new(T)
	name := queueName + "." + adapters.GetMessageLabel(t)
	for _, queueHandlerName := range client.QueuesHandlers {
		if strings.EqualFold(queueHandlerName, name) {
			client.logger.Warn("There is already a handler for the queue %v and message type %v", queueName, adapters.GetMessageLabel(t))
			return
		}
	}

	handlerSvc := queue_receiver.New[T](client.Connection)
	go handlerSvc.HandleMessage(queueName, handler)
	client.QueuesHandlers = append(client.QueuesHandlers, name)
}

func RegisterExchangeHandler[T adapters.Message](exchangeName string, handler func(T)) {
	client := Get()

	t := *new(T)
	name := exchangeName + "." + adapters.GetMessageLabel(t)
	for _, exchangeHandlerName := range client.ExchangeHandlers {
		if strings.EqualFold(exchangeHandlerName, name) {
			client.logger.Warn("There is already a handler for the exchange %v and message type %v", exchangeName, adapters.GetMessageLabel(t))
			return
		}
	}

	handlerSvc := exchange_receiver.New[T](client.Connection)
	go handlerSvc.HandleMessage(exchangeName, handler)
	client.ExchangeHandlers = append(client.ExchangeHandlers, name)
}

func StartListening() {
	var forever chan struct{}
	<-forever
	client := Get()
	client.Close()
}

func (client *RabbitMQClient) Close() {
	if !client.Connection.IsClosed() {
		client.logger.Info("Closing RabbitMQ open connection")
		client.Connection.Close()
	}
}
