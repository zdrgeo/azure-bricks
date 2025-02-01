package envelope

import (
	"errors"
	"time"

	"github.com/zdrgeo/azure-bricks/pubsub"
)

var (
	ErrInvalidEnvelope         = errors.New("invalid envelope")
	ErrInvalidReceivedEnvelope = errors.New("invalid received envelope")
)

type Envelope struct {
	ApplicationProperties map[string]any
	SessionID             *string
	MessageID             *string
	Message               pubsub.Message
}

func NewEnvelope(message pubsub.Message) *Envelope {
	return &Envelope{
		Message: message,
	}
}

func (message *Envelope) Discriminator() pubsub.Discriminator {
	return message.Message.Discriminator()
}

type ReceivedEnvelope struct {
	ApplicationProperties  map[string]any
	EnqueuedSequenceNumber *int64
	EnqueuedTime           *time.Time
	Message                pubsub.Message
}

func NewReceivedEnvelope(message pubsub.Message) *ReceivedEnvelope {
	return &ReceivedEnvelope{
		Message: message,
	}
}

func (message *ReceivedEnvelope) Discriminator() pubsub.Discriminator {
	return message.Message.Discriminator()
}
