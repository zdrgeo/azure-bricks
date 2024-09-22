package processor

import (
	"context"
	"errors"
	"strings"
	"sync"
)

type ProducerFunc[Item any] func(data any) (item Item, foldData any, err error)
type ConsumerFunc[Item any] func(item Item, data any) (foldData any, err error)

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

type processor[Item any] struct {
	producers []*producer[Item]
	consumers []*consumer[Item]
	// quit   chan chan error
}

func NewProcessor[Item any]() *processor[Item] {
	return &processor[Item]{
		producers: []*producer[Item]{},
		consumers: []*consumer[Item]{},
		// quit:   make(chan chan error),
	}
}

func (processor *processor[Item]) AddProducer(producerFunc ProducerFunc[Item], producerData any) {
	producer := &producer[Item]{
		Func: producerFunc,
		Data: producerData,
	}

	processor.producers = append(processor.producers, producer)
}

func (processor *processor[Item]) AddConsumer(consumerFunc ConsumerFunc[Item], consumerData any) {
	consumer := &consumer[Item]{
		Func: consumerFunc,
		Data: consumerData,
	}

	processor.consumers = append(processor.consumers, consumer)
}

func (processor *processor[Item]) Run(c context.Context, size int) error {
	items := make(chan Item, size)

	producersGroup := sync.WaitGroup{}
	consumersGroup := sync.WaitGroup{}

	producersGroup.Add(len(processor.producers))

	for _, producer := range processor.producers {
		go func() {
			defer producersGroup.Done()

			producer.Err = Produce(c, items, producer.Func, producer.Data)
		}()
	}

	consumersGroup.Add(len(processor.consumers))

	for _, consumer := range processor.consumers {
		go func() {
			defer consumersGroup.Done()

			consumer.Err = Consume(context.Background(), items, consumer.Func, consumer.Data)
		}()
	}

	producersGroup.Wait()

	close(items)

	consumersGroup.Wait()

	return processor.runErr()
}

func (processor *processor[Item]) runErr() error {
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

/*
	func (processor *processor[Item]) quit() error {
		err := make(chan error)

		processor.quit <- err

		return <-err
	}
*/

func Produce[Item any](c context.Context, items chan<- Item, producerFunc ProducerFunc[Item], producerData any) error {
	data := producerData

	for {
		item, foldData, err := producerFunc(data)

		if err != nil {
			if errors.Is(err, ErrProducerComplete) {
				return nil
			}

			return err
		}

		data = foldData

		select {
		// case err := <-quit:
		// //	close(items)
		// 	err <- errors.New("error")
		// 	close(err)
		// 	return
		case <-c.Done():
			return c.Err()
		case items <- item:
		}
	}
}

func Consume[Item any](c context.Context, items <-chan Item, consumerFunc ConsumerFunc[Item], consumerData any) error {
	data := consumerData

	for {
		select {
		case <-c.Done():
			return c.Err()
		case item, ok := <-items:
			if !ok {
				return nil
			}

			foldData, err := consumerFunc(item, data)

			if err != nil {
				return err
			}

			data = foldData
		}
	}
}
