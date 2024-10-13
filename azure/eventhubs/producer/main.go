package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"os/signal"
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
			items := [][]byte{}

			eventDataBatch, err := producerClient.NewEventDataBatch(ctx, nil)

			if err != nil {
				log.Panic(err)
			}

			for _, item := range items {
				eventData := &azeventhubs.EventData{
					Body: item,
				}

				if err := eventDataBatch.AddEventData(eventData, nil); err != nil {
					if eventDataBatch.NumEvents() == 0 {
						log.Panic(err)
					}

					if err := producerClient.SendEventDataBatch(ctx, eventDataBatch, nil); err != nil {
						log.Panic(err)
					}

					newEventDataBatch, err := producerClient.NewEventDataBatch(ctx, nil)

					if err != nil {
						log.Panic(err)
					}

					if err := newEventDataBatch.AddEventData(eventData, nil); err != nil {
						log.Panic(err)
					}

					eventDataBatch = newEventDataBatch
				}
			}

			producerClient.SendEventDataBatch(ctx, eventDataBatch, nil)
		}
	}
}

// type checkpoint struct {
// 	index int
// }

type Event struct {
	Bytes []byte
}

type EventBatch struct {
	Events []*Event
}

func newEventBatchProducerFromFile(scanner *bufio.Scanner) (producerFunc processor.ProducerFunc[*EventBatch, any], producerData any) {
	return func(ctx context.Context, data any) (item *EventBatch, foldData any, err error) {
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return nil, nil, err
			} else {
				return nil, nil, processor.ErrProducerComplete
			}
		}

		eventsLimit := viper.GetInt("AZURE_EVENTHUBS_EVENTBATCH_EVENTS_LIMIT")

		events := make([]*Event, 0, eventsLimit)

		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		event := &Event{
			Bytes: scanner.Bytes(),
		}

		events = append(events, event)

		for len(events) < eventsLimit {
			if !scanner.Scan() {
				break
			}

			if err := ctx.Err(); err != nil {
				return nil, nil, err
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
	}, nil
}

func newEventBatchConsumerToEventHubs(producerClient *azeventhubs.ProducerClient) (consumerFunc processor.ConsumerFunc[*EventBatch, any], consumerData any) {
	return func(ctx context.Context, item *EventBatch, data any) (foldData any, err error) {
		eventDataBatch, err := producerClient.NewEventDataBatch(ctx, nil)

		if err != nil {
			return nil, err
		}

		for _, event := range item.Events {
			eventData := &azeventhubs.EventData{
				Body: event.Bytes,
			}

			if err := eventDataBatch.AddEventData(eventData, nil); err != nil {
				if eventDataBatch.NumEvents() == 0 {
					return nil, err
				}

				if err := producerClient.SendEventDataBatch(ctx, eventDataBatch, nil); err != nil {
					return nil, err
				}

				newEventDataBatch, err := producerClient.NewEventDataBatch(ctx, nil)

				if err != nil {
					return nil, err
				}

				if err := newEventDataBatch.AddEventData(eventData, nil); err != nil {
					return nil, err
				}

				eventDataBatch = newEventDataBatch
			}
		}

		if err := producerClient.SendEventDataBatch(ctx, eventDataBatch, nil); err != nil {
			return nil, err
		}

		return nil, nil
	}, nil
}

func newEventBatchProcessorFromFileToEventHubs(scanner *bufio.Scanner, producerClient *azeventhubs.ProducerClient) *processor.Processor[*EventBatch, any, any] {
	eventBatchProcessor := processor.NewProcessor[*EventBatch, any, any]()

	producerFunc, producerData := newEventBatchProducerFromFile(scanner)

	eventBatchProcessor.AddProducer(producerFunc, producerData)

	consumerFunc, consumerData := newEventBatchConsumerToEventHubs(producerClient)

	eventBatchProcessor.AddConsumer(consumerFunc, consumerData)

	return eventBatchProcessor
}

func processFromFileToEventHubs(ctx context.Context, fileName string, producerClient *azeventhubs.ProducerClient) error {
	file, err := os.Open(fileName)

	if err != nil {
		log.Panic(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	eventBatchProcessor := newEventBatchProcessorFromFileToEventHubs(scanner, producerClient)

	if err := eventBatchProcessor.Run(ctx, 10, false); err != nil {
		return err
	}

	return nil
}
