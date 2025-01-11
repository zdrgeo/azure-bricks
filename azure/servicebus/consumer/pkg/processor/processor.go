package processor

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/spf13/viper"
)

type Discriminator string

const (
	EmptyDiscriminator Discriminator = ""
)

type Message interface {
	Discriminator() Discriminator
}

type Publisher interface {
	Publish(message Message) error
}

type Handler interface {
	Discriminator() Discriminator
	Create() Message
	Handle(message Message) error
}

type Dispatcher struct {
	handlers map[Discriminator]Handler
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		handlers: map[Discriminator]Handler{},
	}
}

func (dispatcher *Dispatcher) Register(handler Handler) {
	discriminator := handler.Discriminator()

	dispatcher.handlers[discriminator] = handler
}

func (dispatcher *Dispatcher) Unregister(discriminator Discriminator) {
	delete(dispatcher.handlers, discriminator)
}

func (dispatcher *Dispatcher) Dispatch(discriminator Discriminator) (Handler, bool) {
	handler, ok := dispatcher.handlers[discriminator]

	return handler, ok
}

func UnmarshalDiscriminator(data []byte) (Discriminator, error) {
	message := &struct {
		Discriminator string `json:"discriminator"`
	}{}

	if err := json.Unmarshal(data, &message); err != nil {
		return EmptyDiscriminator, err
	}

	return Discriminator(message.Discriminator), nil
}

func UnmarshalMessage(data []byte, message Message) error {
	return json.Unmarshal(data, message)
}

type Subscriber interface {
	Run(ctx context.Context) error
}

type ServiceBusPublisher struct {
	sender *azservicebus.Sender
}

func NewServiceBusPublisher(sender *azservicebus.Sender) *ServiceBusPublisher {
	return &ServiceBusPublisher{
		sender: sender,
	}
}

func (publisher *ServiceBusPublisher) Publish(ctx context.Context, message Message) error {
	serviceBusMessage := &azservicebus.Message{
		Body: []byte{},
	}

	if err := publisher.sender.SendMessage(ctx, serviceBusMessage, nil); err != nil {
		return err
	}

	return nil
}

type ServiceBusSubscriber struct {
	receiver   *azservicebus.Receiver
	dispatcher *Dispatcher
}

func NewServiceBusSubscriber(receiver *azservicebus.Receiver, dispatcher *Dispatcher) *ServiceBusSubscriber {
	return &ServiceBusSubscriber{
		receiver:   receiver,
		dispatcher: dispatcher,
	}
}

func (subscriber *ServiceBusSubscriber) Run(ctx context.Context) error {
	tick := time.Tick(10 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick:
			serviceBusReceivedMessages, err := subscriber.receiver.ReceiveMessages(ctx, viper.GetInt("AZURE_SERVICEBUS_MESSAGES_LIMIT"), nil)

			if err != nil {
				return err
			}

			for _, serviceBusReceivedMessage := range serviceBusReceivedMessages {
				discriminator, err := UnmarshalDiscriminator(serviceBusReceivedMessage.Body)

				if err != nil {
					deadLetterOptions := &azservicebus.DeadLetterOptions{
						ErrorDescription: to.Ptr(err.Error()),
						Reason:           to.Ptr("UnmarshalDiscriminatorError"),
					}

					if err := subscriber.receiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
						return err
					}
				}

				if handler, ok := subscriber.dispatcher.Dispatch(discriminator); ok {
					message := handler.Create()

					if err := UnmarshalMessage(serviceBusReceivedMessage.Body, message); err != nil {
						deadLetterOptions := &azservicebus.DeadLetterOptions{
							ErrorDescription: to.Ptr(err.Error()),
							Reason:           to.Ptr("UnmarshalMessageError"),
						}

						if err := subscriber.receiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
							return err
						}
					}

					if err := handler.Handle(message); err != nil {
						if err := subscriber.receiver.AbandonMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
							return err
						}
					}
				}

				if err := subscriber.receiver.CompleteMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
					var serviceBusErr *azservicebus.Error

					if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
						continue
					}

					return err
				}
			}
		}
	}
}
