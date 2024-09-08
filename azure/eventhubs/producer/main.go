package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/spf13/viper"
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
	notifyContext, cancelNotify := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelNotify()

	// producerClient, err := azeventhubs.NewProducerClient(viper.GetString("AZURE_EVENTHUBS_NAMESPACE"), viper.GetString("AZURE_EVENTHUBS_EVENTHUB"), credential, nil)
	producerClient, err := azeventhubs.NewProducerClientFromConnectionString(viper.GetString("AZURE_EVENTHUBS_CONNECTION_STRING"), viper.GetString("AZURE_EVENTHUBS_EVENTHUB"), nil)
	if err != nil {
		log.Panic(err)
	}

	defer producerClient.Close(notifyContext)

	ticker := time.NewTicker(time.Minute)

	defer ticker.Stop()

	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			select {
			case <-notifyContext.Done():
				break
			case <-ticker.C:
				events := createEvents()

				eventBatch, err := producerClient.NewEventDataBatch(notifyContext, nil)
				if err != nil {
					log.Panic(err)
				}

				for _, event := range events {
					if err := eventBatch.AddEventData(event, nil); err != nil {
						log.Panic(err)
					}
				}

				producerClient.SendEventDataBatch(notifyContext, eventBatch, nil)
			}
		}
	}()

	<-done
}

func createEvents() []*azeventhubs.EventData {
	return []*azeventhubs.EventData{}
}

func createEvent() *azeventhubs.EventData {
	eventBody := createEventBody()

	event := &azeventhubs.EventData{
		Body: eventBody,
	}

	return event
}

func createEventBody() []byte {
	eventBody := []byte{}

	return eventBody
}
