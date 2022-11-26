package entities

type ReceiverOptions struct {
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
}

type AmqpChannelOptions struct {
	Exclusive  bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
}
