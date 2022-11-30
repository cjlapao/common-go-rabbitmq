package client

import (
	"github.com/cjlapao/common-go-rabbitmq/constants"
	"github.com/cjlapao/common-go/execution_context"
	"github.com/cjlapao/common-go/log"
	"github.com/rabbitmq/amqp091-go"
)

var globalRabbitMQClient *RabbitMQClient

type RabbitMQClient struct {
	logger           *log.Logger
	ConnectionString string
	connection       *amqp091.Connection
	DefaultTimeout   int
	ExchangeHandlers []string
	QueuesHandlers   []string
}

func New(ConnectionString string) *RabbitMQClient {
	ctx := execution_context.Get()
	config := ctx.Configuration
	defaultTimeout := config.GetInt(constants.SENDER_DEFAULT_TIMEOUT)
	if defaultTimeout == 0 {
		defaultTimeout = 5
	}

	client := RabbitMQClient{
		logger:           log.Get(),
		ConnectionString: ConnectionString,
		ExchangeHandlers: make([]string, 0),
		QueuesHandlers:   make([]string, 0),
		DefaultTimeout:   defaultTimeout,
	}

	if client.connection == nil {
		client.Connect()
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

func StartListening() {
	var forever chan struct{}
	<-forever
	client := Get()
	client.Close()
}

func (client *RabbitMQClient) Close() {
	if !client.connection.IsClosed() {
		client.logger.Info("Closing RabbitMQ open connection")
		client.connection.Close()
	}
}

func (client *RabbitMQClient) Connect() error {
	if client.connection == nil || client.connection.IsClosed() {
		conn, err := amqp091.Dial(client.ConnectionString)
		if err != nil {
			client.logger.Exception(err, "failed to connect to rabbitmq server")
			return nil
		}

		client.connection = conn
		client.logger.Info("Connected to RabbitMQ server")
	}
	return nil
}

func (client *RabbitMQClient) GetChannel() (*amqp091.Channel, error) {
	if client.connection.IsClosed() {
		err := client.Connect()
		if err != nil {
			return nil, err
		}
	}

	ch, err := client.connection.Channel()
	if err != nil {
		client.logger.Exception(err, "failed to create channel to rabbitmq server")
	}

	return ch, err
}
