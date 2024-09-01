package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/spf13/viper"
)

var (
	credential *azidentity.DefaultAzureCredential
	client     *azservicebus.Client
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

	_ = credential
	// client, err = azservicebus.NewClient(viper.GetString("AZURE_SERVICEBUS_NAMESPACE"), credential, nil)
	client, err = azservicebus.NewClientFromConnectionString(viper.GetString("AZURE_SERVICEBUS_CONNECTION_STRING"), nil)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	notifyContext, stopNotify := signal.NotifyContext(context.Background(), os.Interrupt)

	defer stopNotify()

	receiver, err := client.NewReceiverForSubscription(viper.GetString("AZURE_SERVICEBUS_TOPIC"), viper.GetString("AZURE_SERVICEBUS_SUBSCRIPTION"), nil)
	if err != nil {
		log.Fatal(err)
	}

	defer receiver.Close(notifyContext)

	ticker := time.NewTicker(1 * time.Minute)

	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			select {
			case <-notifyContext.Done():
				break
			case <-ticker.C:
				messages, err := receiver.ReceiveMessages(notifyContext, 1, nil)
				if err != nil {
					log.Fatal(err)
				}

				for _, message := range messages {
					if err := handleMessage(message); err != nil {
						if err := receiver.AbandonMessage(notifyContext, message, nil); err != nil {
							log.Fatal(err)
						}
					}

					if err := receiver.CompleteMessage(notifyContext, message, nil); err != nil {
						log.Fatal(err)
					}
				}
			}
		}
	}()

	<-done
}

func handleMessage(message *azservicebus.ReceivedMessage) error {
	return handleMessageBody(message.Body)
}

func handleMessageBody(messageBody []byte) error {
	return nil
}