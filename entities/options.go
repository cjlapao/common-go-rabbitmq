package entities

type ReceiverOptions struct {
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
}

type AmqpQueueOptions struct {
	Exclusive  bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
}

type QueueOptions int8

const (
	ExclusiveQueueOption QueueOptions = iota
	DurableQueueOption
	AutoDeleteQueueOption
	InternalQueueOption
	NoWaitQueueOption
)
