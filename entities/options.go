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

type CreateOptions int8

const (
	ExclusiveCreateOption CreateOptions = iota
	DurableCreateOption
	AutoDeleteCreateOption
	InternalCreateOption
	NoWaitCreateOption
)
