package processor

import (
	"context"
	"errors"
	"strings"
	"sync"
)

type ProducerFunc[Item any] func(ctx context.Context, data any) (item Item, foldData any, err error)
type ConsumerFunc[Item any] func(ctx context.Context, item Item, data any) (foldData any, err error)

var ErrProducerComplete = errors.New("producer complete")

type RunError struct {
	ProducerErrs []error
	ConsumerErrs []error
}

func (runErr *RunError) Error() string {
	errMsgs := make([]string, len(runErr.ProducerErrs)+len(runErr.ConsumerErrs))

	for _, producerErr := range runErr.ProducerErrs {
		errMsgs = append(errMsgs, producerErr.Error())
	}

	for _, consumerErr := range runErr.ConsumerErrs {
		errMsgs = append(errMsgs, consumerErr.Error())
	}

	errMsg := strings.Join(errMsgs, "\n")

	return errMsg
}

type producer[Item any] struct {
	Func ProducerFunc[Item]
	Data any
	Err  error
}

type consumer[Item any] struct {
	Func ConsumerFunc[Item]
	Data any
	Err  error
}

type Processor[Item any] struct {
	producers []*producer[Item]
	consumers []*consumer[Item]
}

func NewProcessor[Item any]() *Processor[Item] {
	return &Processor[Item]{
		producers: []*producer[Item]{},
		consumers: []*consumer[Item]{},
	}
}

func (processor *Processor[Item]) AddProducer(producerFunc ProducerFunc[Item], producerData any) {
	producer := &producer[Item]{
		Func: producerFunc,
		Data: producerData,
	}

	processor.producers = append(processor.producers, producer)
}

func (processor *Processor[Item]) AddConsumer(consumerFunc ConsumerFunc[Item], consumerData any) {
	consumer := &consumer[Item]{
		Func: consumerFunc,
		Data: consumerData,
	}

	processor.consumers = append(processor.consumers, consumer)
}

func (processor *Processor[Item]) Run(ctx context.Context, size int, consumersComplete bool) error {
	items := make(chan Item, size)

	producersGroup := sync.WaitGroup{}
	consumersGroup := sync.WaitGroup{}

	producersGroup.Add(len(processor.producers))

	for _, producer := range processor.producers {
		go func() {
			defer producersGroup.Done()

			producer.Err = Produce(ctx, items, producer.Func, producer.Data)
		}()
	}

	var consumeCtx context.Context

	if consumersComplete {
		consumeCtx = context.Background()
	} else {
		consumeCtx = ctx
	}

	consumersGroup.Add(len(processor.consumers))

	for _, consumer := range processor.consumers {
		go func() {
			defer consumersGroup.Done()

			consumer.Err = Consume(consumeCtx, items, consumer.Func, consumer.Data)
		}()
	}

	producersGroup.Wait()

	close(items)

	consumersGroup.Wait()

	return processor.runErr()
}

func (processor *Processor[Item]) runErr() error {
	var producerErrs []error

	for _, producer := range processor.producers {
		if producer.Err != nil {
			producerErrs = append(producerErrs, producer.Err)
		}
	}

	var consumerErrs []error

	for _, consumer := range processor.consumers {
		if consumer.Err != nil {
			consumerErrs = append(consumerErrs, consumer.Err)
		}
	}

	if len(producerErrs) != 0 || len(consumerErrs) != 0 {
		return &RunError{
			ProducerErrs: producerErrs,
			ConsumerErrs: consumerErrs,
		}
	}

	return nil
}

func Produce[Item any](ctx context.Context, items chan<- Item, producerFunc ProducerFunc[Item], producerData any) error {
	data := producerData

	for {
		item, foldData, err := producerFunc(ctx, data)

		if err != nil {
			if errors.Is(err, ErrProducerComplete) {
				return nil
			}

			return err
		}

		data = foldData

		select {
		case <-ctx.Done():
			return ctx.Err()
		case items <- item:
		}
	}
}

func Consume[Item any](ctx context.Context, items <-chan Item, consumerFunc ConsumerFunc[Item], consumerData any) error {
	data := consumerData

	for {
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
