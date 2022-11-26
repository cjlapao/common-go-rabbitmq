package entities

import (
	"bytes"
	"encoding/json"
)

type ReceiverExchangeType int8

const (
	Fanout ReceiverExchangeType = iota
	Direct
	Topic
	Headers
)

func (ReceiverExchangeType ReceiverExchangeType) String() string {
	return toReceiverExchangeTypeString[ReceiverExchangeType]
}

func (oReceiverExchangeType ReceiverExchangeType) FromString(keyType string) ReceiverExchangeType {
	return toReceiverExchangeTypeID[keyType]
}

var toReceiverExchangeTypeString = map[ReceiverExchangeType]string{
	Fanout:  "fanout",
	Direct:  "direct",
	Topic:   "topic",
	Headers: "headers",
}

var toReceiverExchangeTypeID = map[string]ReceiverExchangeType{
	"fanout":  Fanout,
	"direct":  Direct,
	"topic":   Topic,
	"headers": Headers,
}

func (ReceiverExchangeType ReceiverExchangeType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(toReceiverExchangeTypeString[ReceiverExchangeType])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

func (ReceiverExchangeType *ReceiverExchangeType) UnmarshalJSON(b []byte) error {
	var key string
	err := json.Unmarshal(b, &key)
	if err != nil {
		return err
	}

	*ReceiverExchangeType = toReceiverExchangeTypeID[key]
	return nil
}
