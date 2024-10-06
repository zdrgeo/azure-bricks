package processor

import (
	"context"
	"errors"
	"strings"
	"sync"
)

type ProducerFunc[Item any, Data any] func(ctx context.Context, data Data) (item Item, foldData Data, err error)
type ConsumerFunc[Item any, Data any] func(ctx context.Context, item Item, data Data) (foldData Data, err error)

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

type producer[Item any, Data any] struct {
	Func ProducerFunc[Item, Data]
	Data Data
	Err  error
}

type consumer[Item any, Data any] struct {
	Func ConsumerFunc[Item, Data]
	Data Data
	Err  error
}

type Processor[Item any, ProducerData any, ConsumerData any] struct {
	producers []*producer[Item, ProducerData]
	consumers []*consumer[Item, ConsumerData]
}

func NewProcessor[Item any, ProducerData any, ConsumerData any]() *Processor[Item, ProducerData, ConsumerData] {
	return &Processor[Item, ProducerData, ConsumerData]{
		producers: []*producer[Item, ProducerData]{},
		consumers: []*consumer[Item, ConsumerData]{},
	}
}

func (processor *Processor[Item, ProducerData, ConsumerData]) AddProducer(producerFunc ProducerFunc[Item, ProducerData], producerData ProducerData) {
	producer := &producer[Item, ProducerData]{
		Func: producerFunc,
		Data: producerData,
	}

	processor.producers = append(processor.producers, producer)
}

func (processor *Processor[Item, ProducerData, ConsumerData]) AddConsumer(consumerFunc ConsumerFunc[Item, ConsumerData], consumerData ConsumerData) {
	consumer := &consumer[Item, ConsumerData]{
		Func: consumerFunc,
		Data: consumerData,
	}

	processor.consumers = append(processor.consumers, consumer)
}

func (processor *Processor[Item, ProducerData, ConsumerData]) Run(ctx context.Context, size int, consumersRunToCompletion bool) error {
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

	if consumersRunToCompletion {
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

	err := processor.runErr()

	return err
}

func (processor *Processor[Item, ProducerData, ConsumerData]) runErr() error {
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

func Produce[Item any, ProducerData any](ctx context.Context, items chan<- Item, producerFunc ProducerFunc[Item, ProducerData], producerData ProducerData) error {
	data := producerData

	for {
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

func Consume[Item any, ConsumerData any](ctx context.Context, items <-chan Item, consumerFunc ConsumerFunc[Item, ConsumerData], consumerData ConsumerData) error {
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
