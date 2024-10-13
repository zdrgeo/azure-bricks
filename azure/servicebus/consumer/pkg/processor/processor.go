package processor

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/spf13/viper"
)

type Discriminator string

type Message interface {
	Discriminator() Discriminator
}

type Handler interface {
	Discriminator() Discriminator
	Handle(message Message) error
}

type Publisher interface {
	Publish(message Message) error
}

type Dispatcher struct {
	handlers map[Discriminator]Handler
}

type UnregisterFunc func()

func (dispatcher *Dispatcher) Register(handler Handler) UnregisterFunc {
	discriminator := handler.Discriminator()

	dispatcher.handlers[discriminator] = handler

	unregisterFunc := func() {
		dispatcher.Unregister(discriminator)
	}

	return unregisterFunc
}

func (dispatcher *Dispatcher) Unregister(discriminator Discriminator) {
	delete(dispatcher.handlers, discriminator)
}

func (dispatcher *Dispatcher) Dispatch(message Message) error {
	discriminator := message.Discriminator()

	if handler, ok := dispatcher.handlers[discriminator]; ok {
		return handler.Handle(message)
	}

	return nil
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
				var message Message = nil

				if err := subscriber.dispatcher.Dispatch(message); err != nil {
					if err := subscriber.receiver.AbandonMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
						return err
					}
				}

				if err := subscriber.receiver.CompleteMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
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
