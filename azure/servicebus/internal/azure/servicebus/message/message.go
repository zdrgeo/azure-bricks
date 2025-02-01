package message

import (
	"encoding/json"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	emptymessage "github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/empty"
	envelopemessage "github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/envelope"
	"github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/event/em"
	"github.com/zdrgeo/azure-bricks/azure/servicebus/pkg/azure/servicebus"
	"github.com/zdrgeo/azure-bricks/pubsub"
)

func NewMarshalMessageFunc() servicebus.MarshalMessageFunc {
	return func(message pubsub.Message) (*azservicebus.Message, error) {
		body, err := json.Marshal(message)

		if err != nil {
			return nil, err
		}

		serviceBusMessage := &azservicebus.Message{
			Body: body,
		}

		return serviceBusMessage, nil
	}
}

func CreateMessage(discriminator pubsub.Discriminator) pubsub.Message {
	var message pubsub.Message

	switch discriminator {
	case em.DiscriminatorEmployee:
		message = &em.EmployeeEvent{}
	case em.DiscriminatorPosition:
		message = &em.PositionEvent{}
	case em.DiscriminatorRole:
		message = &em.RoleEvent{}
	default:
		message = &emptymessage.Empty{}
	}

	return message
}

type CreateMessageFunc func(discriminator pubsub.Discriminator) pubsub.Message

func NewUnmarshalMessageFunc(createMessageFunc CreateMessageFunc) servicebus.UnmarshalMessageFunc {
	return func(serviceBusReceivedMessage *azservicebus.ReceivedMessage) (pubsub.Message, error) {
		// discriminator := pubsub.Discriminator(serviceBusReceivedMessage.ApplicationProperties["Type"].(string))

		partialMessage := &struct {
			Type string `json:"Type"`
		}{}

		if err := json.Unmarshal(serviceBusReceivedMessage.Body, &partialMessage); err != nil {
			return nil, err
		}

		discriminator := pubsub.Discriminator(partialMessage.Type)

		message := createMessageFunc(discriminator)

		if err := json.Unmarshal(serviceBusReceivedMessage.Body, &message); err != nil {
			return nil, err
		}

		return message, nil
	}
}

func NewMarshalEnvelopeFunc(marshalMessageFunc servicebus.MarshalMessageFunc) servicebus.MarshalMessageFunc {
	return func(message pubsub.Message) (*azservicebus.Message, error) {
		envelope, ok := message.(*envelopemessage.Envelope)

		if !ok {
			return nil, envelopemessage.ErrInvalidEnvelope
		}

		serviceBusMessage, err := marshalMessageFunc(envelope.Message)

		if err != nil {
			return nil, err
		}

		serviceBusMessage.ApplicationProperties = envelope.ApplicationProperties
		serviceBusMessage.SessionID = envelope.SessionID
		serviceBusMessage.MessageID = envelope.MessageID

		return serviceBusMessage, nil
	}
}

func NewUnmarshalReceivedEnvelopeFunc(unmarshalMessageFunc servicebus.UnmarshalMessageFunc) servicebus.UnmarshalMessageFunc {
	return func(serviceBusReceivedMessage *azservicebus.ReceivedMessage) (pubsub.Message, error) {
		message, err := unmarshalMessageFunc(serviceBusReceivedMessage)

		if err != nil {
			return nil, err
		}

		receivedEnvelope := envelopemessage.NewReceivedEnvelope(message)

		receivedEnvelope.ApplicationProperties = serviceBusReceivedMessage.ApplicationProperties
		receivedEnvelope.EnqueuedSequenceNumber = serviceBusReceivedMessage.EnqueuedSequenceNumber
		receivedEnvelope.EnqueuedTime = serviceBusReceivedMessage.EnqueuedTime

		return receivedEnvelope, nil
	}
}
