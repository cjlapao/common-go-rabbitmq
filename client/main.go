package client

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cjlapao/common-go-rabbitmq/constants"
	"github.com/cjlapao/common-go-rabbitmq/entities"
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
	retryFor         int
	currentBackOff   int
	// ExchangeHandlers map[string]*exchange_receiver.ExchangeReceiverService
	// QueuesHandlers map[string]*queue_receiver.QueueReceiverService
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
		retryFor:         5,
		// ExchangeHandlers: make(map[string]*exchange_receiver.ExchangeReceiverService),
		// QueuesHandlers: make(map[string]*queue_receiver.QueueReceiverService),
		DefaultTimeout: defaultTimeout,
	}

	if client.connection == nil {
		err := client.Connect()
		if err != nil {
			return nil
		}
	}

	globalRabbitMQClient = &client
	return globalRabbitMQClient
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
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	client := Get()
	client.Close()

}

func (client *RabbitMQClient) Close() {
	if !client.connection.IsClosed() {
		client.logger.Info("Closing RabbitMQ connection")
		client.connection.Close()
	}
}

func (client *RabbitMQClient) Connect() error {
	attempt := 0
	// Adding ferbernacy initial sequence for back off
	nextBackOff := 1
	previousBackOff := 1

	var err error
	var conn *amqp091.Connection
	for attempt < client.retryFor {
		if client.connection == nil || client.connection.IsClosed() {
			conn, err = amqp091.Dial(client.ConnectionString)
			if err != nil {
				client.logger.Exception(err, "failed to connect to rabbitmq server, retrying...")
				attempt = attempt + 1

				backOffFor := nextBackOff
				// Sleeping before reattempting connection
				time.Sleep(time.Duration(backOffFor * int(time.Second)))
				nextBackOff = previousBackOff + backOffFor
				previousBackOff = backOffFor
			} else {
				break
			}
		}
	}

	if err != nil {
		client.logger.Exception(err, "failed to connect to rabbitmq server, giving up...")
		return err
	}

	client.connection = conn
	client.logger.Info("Connected to RabbitMQ server")

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

func (client *RabbitMQClient) GetQueue(queueName string) (entities.AmqpQueue, error) {
	var result entities.AmqpQueue
	ch, err := client.GetChannel()
	if err != nil {
		return result, err
	}

	if q, err := ch.QueueInspect(queueName); err != nil {
		client.logger.Exception(err, "queue does not exists %v", queueName)
		return result, err
	} else {
		result = entities.AmqpQueue{
			Consumers: q.Consumers,
			Messages:  q.Messages,
			Name:      q.Name,
		}
	}

	return result, nil
}

func (client *RabbitMQClient) CreateQueue(queueName string, options ...entities.CreateOptions) error {
	return client.CreateQueueWithArguments(queueName, entities.AmqpQueueArguments{}, options...)
}

func (client *RabbitMQClient) CreateQueueWithArguments(queueName string, args entities.AmqpQueueArguments, options ...entities.CreateOptions) error {
	ch, err := client.GetChannel()
	if err != nil {
		return err
	}

	isDurable := false
	isAutoDelete := false
	isExclusive := false
	isNoWait := false

	for _, option := range options {
		if option == entities.DurableCreateOption {
			isDurable = true
		}
		if option == entities.AutoDeleteCreateOption {
			isAutoDelete = true
		}
		if option == entities.ExclusiveCreateOption {
			isExclusive = true
		}
		if option == entities.NoWaitCreateOption {
			isNoWait = true
		}
	}

	argsTable := amqp091.Table{}
	if args.QueueExpiresInMs > 0 {
		argsTable["x-expires"] = args.QueueExpiresInMs
	}
	if args.MessageTtlMs > 0 {
		argsTable["x-message-ttl"] = args.MessageTtlMs
	}
	if args.QueueOverflowBehavior != "" {
		argsTable["x-overflow"] = args.QueueOverflowBehavior
	}
	if args.DeadLetterExchange != "" {
		argsTable["x-dead-letter-exchange"] = args.DeadLetterExchange
	}
	if args.DeadLetterRoutingKey != "" {
		argsTable["x-dead-letter-routing-key"] = args.DeadLetterRoutingKey
	}
	if args.MaxLength > 0 {
		argsTable["x-max-length"] = args.MaxLength
	}
	if args.MaxLengthBytes > 0 {
		argsTable["x-max-length-bytes"] = args.MaxLengthBytes
	}
	if args.MaxPriorities > 0 {
		argsTable["x-max-priority"] = args.MaxPriorities
	}
	if args.LazyMode {
		argsTable["x-queue-mode"] = "lazy"
	}
	if args.QueueVersion > 0 {
		argsTable["x-queue-version"] = args.QueueVersion
	}

	if _, err := ch.QueueDeclare(
		queueName,
		isDurable,
		isAutoDelete,
		isExclusive,
		isNoWait,
		argsTable,
	); err != nil {
		client.logger.Exception(err, "error creating queue %v", queueName)
		return err
	}

	return nil
}

func (client *RabbitMQClient) DeleteQueue(queueName string, options ...entities.AmqpQueueDeleteOptions) error {
	ifUnused := false
	ifEmpty := false
	noWait := false

	for _, option := range options {
		if option == entities.DeleteIfUnused {
			ifUnused = true
		}
		if option == entities.DeleteIfEmpty {
			ifEmpty = true
		}
		if option == entities.DeleteNoWait {
			noWait = true
		}
	}

	ch, err := client.GetChannel()
	if err != nil {
		return err
	}

	if _, err := ch.QueueDelete(queueName, ifUnused, ifEmpty, noWait); err != nil {
		client.logger.Exception(err, "error deleting queue %v", queueName)
		return err
	}

	return nil
}

func (client *RabbitMQClient) UnbindQueue(queueName string, routingKey string, exchange string) error {
	ch, err := client.GetChannel()
	if err != nil {
		return err
	}

	if err := ch.QueueUnbind(queueName, routingKey, exchange, nil); err != nil {
		client.logger.Exception(err, "error unbinding queue %v from exchange %v", queueName, exchange)
		return err
	}

	return nil
}

func (client *RabbitMQClient) BindQueue(queueName string, routingKey string, exchange string) error {
	ch, err := client.GetChannel()
	if err != nil {
		return err
	}

	if err := ch.QueueBind(queueName, routingKey, exchange, false, nil); err != nil {
		client.logger.Exception(err, "error binding queue %v from exchange %v", queueName, exchange)
		return err
	}

	return nil
}

func (client *RabbitMQClient) PurgeQueue(queueName string, routingKey string, exchange string) error {
	ch, err := client.GetChannel()
	if err != nil {
		return err
	}

	if _, err := ch.QueuePurge(queueName, false); err != nil {
		client.logger.Exception(err, "error purging queue %v from exchange %v", queueName, exchange)
		return err
	}

	return nil
}

func (client *RabbitMQClient) CreateExchange(exchangeName string, exchangeType entities.ReceiverExchangeType, options ...entities.CreateOptions) error {
	isDurable := false
	isAutoDelete := false
	isExclusive := false
	isNoWait := false

	for _, option := range options {
		if option == entities.DurableCreateOption {
			isDurable = true
		}
		if option == entities.AutoDeleteCreateOption {
			isAutoDelete = true
		}
		if option == entities.ExclusiveCreateOption {
			isExclusive = true
		}
		if option == entities.NoWaitCreateOption {
			isNoWait = true
		}
	}

	ch, err := client.GetChannel()
	if err != nil {
		return err
	}

	if err := ch.ExchangeDeclare(
		exchangeName,
		exchangeType.String(),
		isDurable,
		isAutoDelete,
		isExclusive,
		isNoWait,
		nil,
	); err != nil {
		client.logger.Exception(err, "error creating exchange %v", exchangeName)
		return err
	}

	return nil
}

func (client *RabbitMQClient) DeleteExchange(exchangeName string, options ...entities.AmqpQueueDeleteOptions) error {
	ifUnused := false
	noWait := false

	for _, option := range options {
		if option == entities.DeleteIfUnused {
			ifUnused = true
		}
		if option == entities.DeleteNoWait {
			noWait = true
		}
	}

	ch, err := client.GetChannel()
	if err != nil {
		return err
	}

	if err := ch.ExchangeDelete(exchangeName, ifUnused, noWait); err != nil {
		client.logger.Exception(err, "error deleting exchange %v", exchangeName)
		return err
	}

	return nil
}

func (client *RabbitMQClient) UnbindFromExchange(queueName string, routingKey string, exchange string) error {
	ch, err := client.GetChannel()
	if err != nil {
		return err
	}

	if err := ch.ExchangeUnbind(queueName, routingKey, exchange, false, nil); err != nil {
		client.logger.Exception(err, "error unbinding queue %v from exchange %v", queueName, exchange)
		return err
	}

	return nil
}

func (client *RabbitMQClient) BindToExchange(queueName string, routingKey string, exchange string) error {
	ch, err := client.GetChannel()
	if err != nil {
		return err
	}

	if err := ch.ExchangeBind(queueName, routingKey, exchange, false, nil); err != nil {
		client.logger.Exception(err, "error binding queue %v from exchange %v", queueName, exchange)
		return err
	}

	return nil
}
