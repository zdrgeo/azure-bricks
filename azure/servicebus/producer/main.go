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
}

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelCtx()

	sender, err := client.NewSender(viper.GetString("AZURE_SERVICEBUS_TOPIC"), nil)

	if err != nil {
		log.Panic(err)
	}

	defer sender.Close(ctx)

	tick := time.Tick(1 * time.Minute)

	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick:
			message := createMessage()

			if err := sender.SendMessage(ctx, message, nil); err != nil {
				log.Panic(err)
			}
		}
	}
}

func createMessage() *azservicebus.Message {
	messageBody := createMessageBody()

	message := &azservicebus.Message{
		Body: messageBody,
	}

	return message
}

func createMessageBody() []byte {
	messageBody := []byte{}

	return messageBody
}
