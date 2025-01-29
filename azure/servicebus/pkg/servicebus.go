package servicebus

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/zdrgeo/azure-bricks/pubsub"
)

type MarshalMessageFunc func(message pubsub.Message) (*azservicebus.Message, error)

type PublisherOptions struct{}

type Publisher struct {
	sender             *azservicebus.Sender
	marshalMessageFunc MarshalMessageFunc
	logger             *slog.Logger
	options            *PublisherOptions
}

func NewPublisher(sender *azservicebus.Sender, marshalMessageFunc MarshalMessageFunc, logger *slog.Logger, options *PublisherOptions) *Publisher {
	return &Publisher{
		sender:             sender,
		marshalMessageFunc: marshalMessageFunc,
		logger:             logger,
		options:            options,
	}
}

func (publisher *Publisher) Publish(ctx context.Context, message pubsub.Message) error {
	serviceBusMessage, err := publisher.marshalMessageFunc(message)

	if err != nil {
		return err
	}

	if err := publisher.sender.SendMessage(ctx, serviceBusMessage, nil); err != nil {
		return err
	}

	return nil
}

type UnmarshalMessageFunc func(serviceBusReceivedMessage *azservicebus.ReceivedMessage) (pubsub.Message, error)

type SubscriberOptions struct {
	Interval      time.Duration
	MessagesLimit int
}

type Subscriber struct {
	receiver             *azservicebus.Receiver
	dispatcher           *pubsub.Dispatcher
	unmarshalMessageFunc UnmarshalMessageFunc
	logger               *slog.Logger
	options              *SubscriberOptions
}

func NewSubscriber(receiver *azservicebus.Receiver, dispatcher *pubsub.Dispatcher, unmarshalMessageFunc UnmarshalMessageFunc, logger *slog.Logger, options *SubscriberOptions) *Subscriber {
	return &Subscriber{
		receiver:             receiver,
		dispatcher:           dispatcher,
		unmarshalMessageFunc: unmarshalMessageFunc,
		logger:               logger,
		options:              options,
	}
}

func (subscriber *Subscriber) Run(ctx context.Context) error {
	interval := 1 * time.Minute
	messagesLimit := 1

	if subscriber.options != nil {
		if subscriber.options.Interval > 0 {
			interval = subscriber.options.Interval
		}

		if subscriber.options.MessagesLimit > 0 {
			messagesLimit = subscriber.options.MessagesLimit
		}
	}

	tick := time.Tick(interval)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick:
			serviceBusReceivedMessages, err := subscriber.receiver.ReceiveMessages(ctx, messagesLimit, nil)

			if err != nil {
				return err
			}

			for _, serviceBusReceivedMessage := range serviceBusReceivedMessages {
				message, err := subscriber.unmarshalMessageFunc(serviceBusReceivedMessage)

				if err != nil {
					deadLetterOptions := &azservicebus.DeadLetterOptions{
						ErrorDescription: to.Ptr(err.Error()),
						Reason:           to.Ptr("UnmarshalMessageError"),
					}

					if err := subscriber.receiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
						var serviceBusErr *azservicebus.Error

						if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
							subscriber.logger.Warn("message lock was lost while trying to dead letter the message")

							continue
						}

						return err
					}

					subscriber.logger.Error("message was dead lettered", "error", err)

					continue
				}

				discriminator := message.Discriminator()

				if handler, ok := subscriber.dispatcher.Dispatch(discriminator); ok {
					if err := handler.Handle(message); err != nil {
						if err := subscriber.receiver.AbandonMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
							var serviceBusErr *azservicebus.Error

							if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
								subscriber.logger.Warn("message lock was lost while trying to abandon the message")

								continue
							}

							return err
						}

						subscriber.logger.Error("message was abandoned", "error", err)

						continue
					}
				} else {
					subscriber.logger.Info("message handler was not found", "discriminator", discriminator)
				}

				if err := subscriber.receiver.CompleteMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
					var serviceBusErr *azservicebus.Error

					if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
						subscriber.logger.Warn("message lock was lost while trying to complete the message")

						continue
					}

					return err
				}
			}
		}
	}
}
