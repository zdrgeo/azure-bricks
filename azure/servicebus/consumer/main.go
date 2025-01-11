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
	"github.com/zdrgeo/azure-bricks/azure-servicebus-consumer/pkg/processor"
)

var (
	credential *azidentity.DefaultAzureCredential
	client     *azservicebus.Client

	dispatcher *processor.Dispatcher
)

const (
	EmployeeDiscriminator processor.Discriminator = "employee"
)

type EmployeeMessage struct{}

func (message *EmployeeMessage) Discriminator() processor.Discriminator {
	return EmployeeDiscriminator
}

type EmployeeHandler struct{}

func (handler *EmployeeHandler) Discriminator() processor.Discriminator {
	return EmployeeDiscriminator
}

func (handler *EmployeeHandler) Create() processor.Message {
	return &EmployeeMessage{}
}

func (handler *EmployeeHandler) Handle(message processor.Message) error {
	return nil
}

func init() {
	viper.AddConfigPath(".")
	// viper.SetConfigFile(".env")
	// viper.SetConfigName("config")
	// viper.SetConfigType("env") // "env", "json", "yaml"
	viper.SetEnvPrefix("demo")
	viper.AutomaticEnv()

	viper.SetDefault("AZURE_SERVICEBUS_SESSION_LIMIT", 10)
	viper.SetDefault("AZURE_SERVICEBUS_MESSAGE_LIMIT", 1)

	if err := viper.ReadInConfig(); err != nil {
		log.Panic(err)
	}

	var err error

	credential, err = azidentity.NewDefaultAzureCredential(nil)

	if err != nil {
		log.Panic(err)
	}

	_ = credential
	// client, err = azservicebus.NewClient(viper.GetString("AZURE_SERVICEBUS_NAMESPACE"), credential, nil)
	client, err = azservicebus.NewClientFromConnectionString(viper.GetString("AZURE_SERVICEBUS_CONNECTION_STRING"), nil)

	if err != nil {
		log.Panic(err)
	}

	dispatcher = &processor.Dispatcher{}

	employeeHandler := &EmployeeHandler{}

	dispatcher.Register(employeeHandler)
}

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelCtx()

	receiver, err := client.NewReceiverForSubscription(viper.GetString("AZURE_SERVICEBUS_TOPIC"), viper.GetString("AZURE_SERVICEBUS_SUBSCRIPTION"), nil)

	if err != nil {
		log.Panic(err)
	}

	defer receiver.Close(ctx)

	tick := time.Tick(10 * time.Second)

	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick:
			serviceBusReceivedMessages, err := receiver.ReceiveMessages(ctx, viper.GetInt("AZURE_SERVICEBUS_MESSAGES_LIMIT"), nil)

			if err != nil {
				log.Panic(err)
			}

			for _, serviceBusReceivedMessage := range serviceBusReceivedMessages {
				discriminator, err := processor.UnmarshalDiscriminator(serviceBusReceivedMessage.Body)

				if err != nil {
					if err := receiver.AbandonMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
						log.Panic(err)
					}
				}

				if handler, ok := dispatcher.Dispatch(discriminator); ok {
					message := handler.Create()

					if err := processor.UnmarshalMessage(serviceBusReceivedMessage.Body, message); err != nil {
						if err := receiver.AbandonMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
							log.Panic(err)
						}
					}

					if err := handler.Handle(message); err != nil {
						if err := receiver.AbandonMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
							log.Panic(err)
						}
					}
				}

				if err := receiver.CompleteMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
					log.Panic(err)
				}
			}
		}
	}
}
