package entities

type AmqpQueue struct {
	Consumers int
	Messages  int
	Name      string
}

type AmqpQueueDeleteOptions int8

const (
	DeleteIfUnused AmqpQueueDeleteOptions = iota
	DeleteIfEmpty
	DeleteNoWait
)

type AmqpQueueArguments struct {
	QueueExpiresInMs      int
	MessageTtlMs          int
	QueueOverflowBehavior AmqpQueueQueueOverflowType
	DeadLetterExchange    string
	DeadLetterRoutingKey  string
	MaxLength             int
	MaxLengthBytes        int
	MaxPriorities         int
	LazyMode              bool
	QueueVersion          int
}

type AmqpQueueQueueOverflowType string

const (
	QueueOverflowNone             = ""
	QueueOverflowDropHead         = "drop-head"
	QueueOverflowRejectPublish    = "reject-publish"
	QueueOverflowRejectPublishDlx = "reject-publish-dlx"
)
