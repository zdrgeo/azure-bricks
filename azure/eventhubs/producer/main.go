package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/spf13/viper"
	"github.com/zdrgeo/azure-bricks/azure-eventhubs-producer/pkg/processor"
)

var (
	credential *azidentity.DefaultAzureCredential
)

func init() {
	viper.AddConfigPath(".")
	// viper.SetConfigFile(".env")
	// viper.SetConfigName("config")
	// viper.SetConfigType("env") // "env", "json", "yaml"
	viper.SetEnvPrefix("demo")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Panic(err)
	}

	var err error

	credential, err = azidentity.NewDefaultAzureCredential(nil)

	if err != nil {
		log.Panic(err)
	}
}

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelCtx()

	// producerClient, err := azeventhubs.NewProducerClient(viper.GetString("AZURE_EVENTHUBS_NAMESPACE"), viper.GetString("AZURE_EVENTHUBS_EVENTHUB"), credential, nil)
	producerClient, err := azeventhubs.NewProducerClientFromConnectionString(viper.GetString("AZURE_EVENTHUBS_CONNECTION_STRING"), viper.GetString("AZURE_EVENTHUBS_EVENTHUB"), nil)

	if err != nil {
		log.Panic(err)
	}

	defer producerClient.Close(ctx)

	tick := time.Tick(1 * time.Minute)

	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick:
			events := []*azeventhubs.EventData{}

			eventBatch, err := producerClient.NewEventDataBatch(ctx, nil)

			if err != nil {
				log.Panic(err)
			}

			for _, event := range events {
				if err := eventBatch.AddEventData(event, nil); err != nil {
					log.Panic(err)
				}
			}

			producerClient.SendEventDataBatch(ctx, eventBatch, nil)
		}
	}
}

func compose() {
	file, err := os.Open("")

	if err != nil {
		log.Panic(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	_ = scanner
}

type checkpoint struct {
	index int
}

type Event struct {
	Bytes []byte
}

type EventBatch struct {
	Events []*Event
}

const eventBatchLimit = 10

func newEventBatchProducer(scanner *bufio.Scanner) processor.ProducerFunc[*EventBatch] {
	return func(ctx context.Context, data any) (item *EventBatch, foldData any, err error) {
		events := make([]*Event, 0, eventBatchLimit)

		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return nil, nil, err
			} else {
				return nil, nil, processor.ErrProducerComplete
			}
		}

		event := &Event{
			Bytes: scanner.Bytes(),
		}

		events = append(events, event)

		for len(events) < eventBatchLimit {
			if err := ctx.Err(); err != nil {
				break
			}

			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					break
				} else {
					break
				}
			}

			event := &Event{
				Bytes: scanner.Bytes(),
			}

			events = append(events, event)
		}

		eventBatch := &EventBatch{
			Events: events,
		}

		return eventBatch, nil, nil
	}
}

func newEventBatchConsumer(producerClient *azeventhubs.ProducerClient) processor.ConsumerFunc[*EventBatch] {
	return func(ctx context.Context, item *EventBatch, data any) (foldData any, err error) {
		eventBatch, err := producerClient.NewEventDataBatch(ctx, nil)

		if err != nil {
			return nil, err
		}

		for _, event := range item.Events {
			eventData := &azeventhubs.EventData{
				Body: event.Bytes,
			}

			if err := eventBatch.AddEventData(eventData, nil); err != nil {
				return nil, err
			}
		}

		return nil, producerClient.SendEventDataBatch(ctx, eventBatch, nil)
	}
}

func RunAlt[Item any](ctx context.Context, size int, producerFunc processor.ProducerFunc[*EventBatch], consumerFuncs []processor.ConsumerFunc[*EventBatch]) {
	items := make(chan *EventBatch, size)

	go func() {
		defer close(items)

		err := processor.Produce(ctx, items, producerFunc, nil)

		if err != nil {
			log.Panic(err)
		}
	}()

	consumerGroups := sync.WaitGroup{}

	consumerGroups.Add(len(consumerFuncs))

	for _, consumerFunc := range consumerFuncs {
		go func() {
			defer consumerGroups.Done()

			err := processor.Consume(context.Background(), items, consumerFunc, nil)

			if err != nil {
				log.Panic(err)
			}
		}()
	}

	consumerGroups.Wait()
}

func Run[Item any](ctx context.Context, size int, producerFuncs []processor.ProducerFunc[*EventBatch], consumerFuncs []processor.ConsumerFunc[*EventBatch]) {
	items := make(chan *EventBatch, size)

	producersGroup := sync.WaitGroup{}
	consumersGroup := sync.WaitGroup{}

	producersGroup.Add(len(producerFuncs))

	for _, producerFunc := range producerFuncs {
		go func() {
			defer producersGroup.Done()

			err := processor.Produce(ctx, items, producerFunc, nil)

			if err != nil {
				log.Panic(err)
			}
		}()
	}

	consumersGroup.Add(len(consumerFuncs))

	for _, consumerFunc := range consumerFuncs {
		go func() {
			defer consumersGroup.Done()

			err := processor.Consume(context.Background(), items, consumerFunc, nil)

			if err != nil {
				log.Panic(err)
			}
		}()
	}

	producersGroup.Wait()

	close(items)

	consumersGroup.Wait()
}
