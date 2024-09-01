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
		log.Fatal(err)
	}

	var err error

	credential, err = azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatal(err)
	}

	// client, err = container.NewClient(viper.GetString("AZURE_STORAGE_CONTAINER"), credential, nil)
	client, err = container.NewClientFromConnectionString(viper.GetString("AZURE_STORAGE_CONNECTION_STRING"), viper.GetString("AZURE_STORAGE_CONTAINER"), nil)
	if err != nil {
		log.Fatal(err)
	}

	checkpointStore, err = checkpoints.NewBlobStore(client, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	notifyContext, stopNotify := signal.NotifyContext(context.Background(), os.Interrupt)

	defer stopNotify()

	// consumerClient, err := azeventhubs.NewConsumerClient(viper.GetString("AZURE_EVENTHUBS_NAMESPACE"), viper.GetString("AZURE_EVENTHUBS_EVENTHUB"), azeventhubs.DefaultConsumerGroup /* viper.GetString("AZURE_EVENTHUBS_CONSUMERGROUP") */, credential, nil)
	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(viper.GetString("AZURE_EVENTHUBS_CONNECTION_STRING"), viper.GetString("AZURE_EVENTHUBS_EVENTHUB"), azeventhubs.DefaultConsumerGroup /* viper.GetString("AZURE_EVENTHUBS_CONSUMERGROUP") */, nil)
	if err != nil {
		log.Fatal(err)
	}

	defer consumerClient.Close(notifyContext)

	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			processorPartitionClient := processor.NextPartitionClient(notifyContext)

			if processorPartitionClient == nil {
				break
			}

			go func() {
				defer processorPartitionClient.Close(notifyContext)

				for {
					waitContext, stopWait := context.WithTimeout(context.Background(), time.Minute)

					events, err := processorPartitionClient.ReceiveEvents(waitContext, 100, nil)

					stopWait()

					if err != nil && !errors.Is(err, context.DeadlineExceeded) {
						var eventHubsErr *azeventhubs.Error

						if errors.As(err, &eventHubsErr) && eventHubsErr.Code == azeventhubs.ErrorCodeOwnershipLost {
							return
						}

						log.Fatal(err)
					}

					for _, event := range events {
						handleEvent(event)
					}

					if len(events) != 0 {
						if err := processorPartitionClient.UpdateCheckpoint(notifyContext, events[len(events)-1], nil); err != nil {
							log.Fatal(err)
						}
					}
				}
			}()
		}
	}()

	if err := processor.Run(notifyContext); err != nil {
		log.Fatal(err)
	}
}

func handleEvent(event *azeventhubs.ReceivedEventData) error {
	return handleEventBody(event.Body)
}

func handleEventBody(eventBody []byte) error {
	return nil
}
