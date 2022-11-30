package entities

import (
	"bytes"
	"encoding/json"
)

type SenderMessageType int8

const (
	QueueMessage SenderMessageType = iota
	ExchangeMessage
)

func (SenderMessageType SenderMessageType) String() string {
	return toSenderMessageTypeString[SenderMessageType]
}

func (oSenderMessageType SenderMessageType) FromString(keyType string) SenderMessageType {
	return toSenderMessageTypeID[keyType]
}

var toSenderMessageTypeString = map[SenderMessageType]string{
	QueueMessage:    "QueueMessage",
	ExchangeMessage: "ExchangeMessage",
}

var toSenderMessageTypeID = map[string]SenderMessageType{
	"QueueMessage":    QueueMessage,
	"ExchangeMessage": ExchangeMessage,
}

func (SenderMessageType SenderMessageType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(toSenderMessageTypeString[SenderMessageType])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

func (SenderMessageType *SenderMessageType) UnmarshalJSON(b []byte) error {
	var key string
	err := json.Unmarshal(b, &key)
	if err != nil {
		return err
	}

	*SenderMessageType = toSenderMessageTypeID[key]
	return nil
}
