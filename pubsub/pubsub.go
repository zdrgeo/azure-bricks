package pubsub

import (
	"context"
	"errors"
)

const (
	DiscriminatorEmpty Discriminator = ""
)

var (
	ErrInvalidDiscriminator = errors.New("invalid discriminator")
)

type Discriminator string

type Message interface {
	Discriminator() Discriminator
}

type Publisher interface {
	Publish(message Message) error
}

type Handler interface {
	Discriminator() Discriminator
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
