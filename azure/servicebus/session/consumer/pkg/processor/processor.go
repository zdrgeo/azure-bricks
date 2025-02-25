package processor

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type Discriminator string

const (
	DiscriminatorEmpty Discriminator = ""
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

type Subscriber interface {
	Run(ctx context.Context) error
}

type MarshalMessageFunc func(message Message) ([]byte, error)

type PublisherOptions struct{}

type ServiceBusPublisher struct {
	sender             *azservicebus.Sender
	marshalMessageFunc MarshalMessageFunc
	options            *PublisherOptions
}

func NewServiceBusPublisher(sender *azservicebus.Sender, marshalMessageFunc MarshalMessageFunc, options *PublisherOptions) *ServiceBusPublisher {
	return &ServiceBusPublisher{
		sender:             sender,
		marshalMessageFunc: marshalMessageFunc,
		options:            options,
	}
}

func (publisher *ServiceBusPublisher) Publish(ctx context.Context, message Message) error {
	body, err := publisher.marshalMessageFunc(message)

	if err != nil {
		return err
	}

	serviceBusMessage := &azservicebus.Message{
		Body: body,
	}

	if err := publisher.sender.SendMessage(ctx, serviceBusMessage, nil); err != nil {
		return err
	}

	return nil
}

type UnmarshalDiscriminatorFunc func(body []byte, discriminator *Discriminator) error

type UnmarshalMessageFunc func(body []byte, message Message) error

type SubscriberOptions struct {
	Interval      time.Duration
	MessagesLimit int
}

type ServiceBusSubscriber struct {
	receiver                   *azservicebus.Receiver
	dispatcher                 *Dispatcher
	unmarshalDiscriminatorFunc UnmarshalDiscriminatorFunc
	unmarshalMessageFunc       UnmarshalMessageFunc
	options                    *SubscriberOptions
}

func NewServiceBusSubscriber(receiver *azservicebus.Receiver, dispatcher *Dispatcher, unmarshalDiscriminatorFunc UnmarshalDiscriminatorFunc, unmarshalMessageFunc UnmarshalMessageFunc, options *SubscriberOptions) *ServiceBusSubscriber {
	return &ServiceBusSubscriber{
		receiver:                   receiver,
		dispatcher:                 dispatcher,
		unmarshalDiscriminatorFunc: unmarshalDiscriminatorFunc,
		unmarshalMessageFunc:       unmarshalMessageFunc,
		options:                    options,
	}
}

func (subscriber *ServiceBusSubscriber) Run(ctx context.Context) error {
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
				discriminator := DiscriminatorEmpty

				if err := subscriber.unmarshalDiscriminatorFunc(serviceBusReceivedMessage.Body, &discriminator); err != nil {
					deadLetterOptions := &azservicebus.DeadLetterOptions{
						ErrorDescription: to.Ptr(err.Error()),
						Reason:           to.Ptr("UnmarshalDiscriminatorError"),
					}

					if err := subscriber.receiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
						var serviceBusErr *azservicebus.Error

						if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
							continue
						}

						return err
					}
				}

				if handler, ok := subscriber.dispatcher.Dispatch(discriminator); ok {
					message := handler.Create()

					if err := subscriber.unmarshalMessageFunc(serviceBusReceivedMessage.Body, message); err != nil {
						deadLetterOptions := &azservicebus.DeadLetterOptions{
							ErrorDescription: to.Ptr(err.Error()),
							Reason:           to.Ptr("UnmarshalMessageError"),
						}

						if err := subscriber.receiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
							var serviceBusErr *azservicebus.Error

							if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
								continue
							}

							return err
						}
					}

					if err := handler.Handle(message); err != nil {
						if err := subscriber.receiver.AbandonMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
							var serviceBusErr *azservicebus.Error

							if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
								continue
							}

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

type ProducerFunc[Item any, Data any] func(ctx context.Context, data Data) (item Item, foldData Data, err error)
type ConsumerFunc[Item any, Data any] func(ctx context.Context, item Item, data Data) (foldData Data, err error)

var ErrProducerComplete = errors.New("producer complete")

func Produce[Item any, ProducerData any](ctx context.Context, pipes <-chan chan Item, producerFunc ProducerFunc[Item, ProducerData], producerData ProducerData) error {
	data := producerData

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case items, ok := <-pipes:
			if !ok {
				return nil
			}

			item, foldData, err := producerFunc(ctx, data)

			if err != nil {
				if errors.Is(err, ErrProducerComplete) {
					return nil
				}

				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case items <- item:
			}

			data = foldData
		}
	}
}

func Consume[Item any, ConsumerData any](ctx context.Context, pipes chan<- chan Item, items chan Item, consumerFunc ConsumerFunc[Item, ConsumerData], consumerData ConsumerData) error {
	data := consumerData

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case pipes <- items:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case item, ok := <-items:
				if !ok {
					return nil
				}

				foldData, err := consumerFunc(ctx, item, data)

				if err != nil {
					return err
				}

				data = foldData
			}
		}
	}
}

func Run[Item any](ctx context.Context, size int, producerFuncs []ProducerFunc[Item, any], consumerFuncs []ConsumerFunc[Item, any]) {
	pipes := make(chan chan Item)

	// defer close(pipes)

	producersGroup := sync.WaitGroup{}
	consumersGroup := sync.WaitGroup{}

	producersGroup.Add(len(producerFuncs))

	for _, producerFunc := range producerFuncs {
		go func() {
			defer producersGroup.Done()

			err := Produce(ctx, pipes, producerFunc, nil)

			if err != nil {
				log.Panic(err)
			}
		}()
	}

	consumersGroup.Add(len(consumerFuncs))

	for _, consumerFunc := range consumerFuncs {
		go func() {
			defer consumersGroup.Done()

			items := make(chan Item)

			// defer close(items)

			err := Consume(ctx, pipes, items, consumerFunc, nil)

			if err != nil {
				log.Panic(err)
			}
		}()
	}

	producersGroup.Wait()
	consumersGroup.Wait()
}
