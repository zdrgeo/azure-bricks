package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/spf13/viper"
)

var (
	credential      *azidentity.DefaultAzureCredential
	client          *container.Client
	checkpointStore *checkpoints.BlobStore
	consumerClient  *azeventhubs.ConsumerClient
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

	// client, err = container.NewClient(viper.GetString("AZURE_STORAGE_CONTAINER"), credential, nil)
	client, err = container.NewClientFromConnectionString(viper.GetString("AZURE_STORAGE_CONNECTION_STRING"), viper.GetString("AZURE_STORAGE_CONTAINER"), nil)

	if err != nil {
		log.Panic(err)
	}

	checkpointStore, err = checkpoints.NewBlobStore(client, nil)

	if err != nil {
		log.Panic(err)
	}
}

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelCtx()

	// consumerClient, err := azeventhubs.NewConsumerClient(viper.GetString("AZURE_EVENTHUBS_NAMESPACE"), viper.GetString("AZURE_EVENTHUBS_EVENTHUB"), azeventhubs.DefaultConsumerGroup /* viper.GetString("AZURE_EVENTHUBS_CONSUMERGROUP") */, credential, nil)
	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(viper.GetString("AZURE_EVENTHUBS_CONNECTION_STRING"), viper.GetString("AZURE_EVENTHUBS_EVENTHUB"), azeventhubs.DefaultConsumerGroup /* viper.GetString("AZURE_EVENTHUBS_CONSUMERGROUP") */, nil)

	if err != nil {
		log.Panic(err)
	}

	defer consumerClient.Close(ctx)

	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)

	if err != nil {
		log.Panic(err)
	}

	go func() {
		for processorPartitionClient := processor.NextPartitionClient(ctx); processorPartitionClient != nil; processorPartitionClient = processor.NextPartitionClient(ctx) {
			go func() {
				defer processorPartitionClient.Close(ctx)

				for {
					events, err := func() ([]*azeventhubs.ReceivedEventData, error) {
						context, cancel := context.WithTimeout(context.Background(), time.Minute)

						defer cancel()

						return processorPartitionClient.ReceiveEvents(context, 100, nil)
					}()

					if err != nil && !errors.Is(err, context.DeadlineExceeded) {
						var eventHubsErr *azeventhubs.Error

						if errors.As(err, &eventHubsErr) && eventHubsErr.Code == azeventhubs.ErrorCodeOwnershipLost {
							return
						}

						log.Panic(err)
					}

					if len(events) != 0 {
						if err := handleEvents(events); err != nil {
							log.Println(err)
						}

						if err := processorPartitionClient.UpdateCheckpoint(ctx, events[len(events)-1], nil); err != nil {
							log.Panic(err)
						}
					}
				}
			}()
		}
	}()

	if err := processor.Run(ctx); err != nil {
		log.Panic(err)
	}
}

func handleEvents(events []*azeventhubs.ReceivedEventData) error {
	for _, event := range events {
		if err := handleEvent(event); err != nil {
			return err
		}
	}

	return nil
}

func handleEvent(event *azeventhubs.ReceivedEventData) error {
	return handleEventBody(event.Body)
}

func handleEventBody(eventBody []byte) error {
	_ = eventBody

	return nil
}
