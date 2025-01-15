package main

import (
	"context"
	"encoding/json"
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
	DiscriminatorEmployee processor.Discriminator = "employee"
)

type EmployeeMessage struct{}

func (message *EmployeeMessage) Discriminator() processor.Discriminator {
	return DiscriminatorEmployee
}

type EmployeeHandler struct{}

func (handler *EmployeeHandler) Discriminator() processor.Discriminator {
	return DiscriminatorEmployee
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

	viper.SetDefault("AZURE_SERVICEBUS_INTERVAL", 10*time.Second)
	viper.SetDefault("AZURE_SERVICEBUS_MESSAGES_LIMIT", 1)

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

	dispatcher = processor.NewDispatcher()

	dispatcher.Register(&EmployeeHandler{})
}

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelCtx()

	receiverOptions := &azservicebus.ReceiverOptions{
		ReceiveMode: azservicebus.ReceiveModePeekLock,
	}

	receiver, err := client.NewReceiverForSubscription(viper.GetString("AZURE_SERVICEBUS_TOPIC"), viper.GetString("AZURE_SERVICEBUS_SUBSCRIPTION"), receiverOptions)

	if err != nil {
		log.Panic(err)
	}

	defer receiver.Close(ctx)

	subscriberOptions := &processor.SubscriberOptions{
		Interval:      viper.GetDuration("AZURE_SERVICEBUS_INTERVAL"),
		MessagesLimit: viper.GetInt("AZURE_SERVICEBUS_MESSAGES_LIMIT"),
	}

	serviceBusSubscriber := processor.NewServiceBusSubscriber(receiver, dispatcher, unmarshalDiscriminator, unmarshalMessage, subscriberOptions)

	if err := serviceBusSubscriber.Run(ctx); err != nil {
		log.Panic(err)
	}
}

func unmarshalDiscriminator(data []byte, discriminator *processor.Discriminator) error {
	partialMessage := &struct {
		Type string `json:"Type"`
	}{}

	if err := json.Unmarshal(data, &partialMessage); err != nil {
		return err
	}

	discriminator = (*processor.Discriminator)(&partialMessage.Type)

	return nil
}

func unmarshalMessage(data []byte, message processor.Message) error {
	return json.Unmarshal(data, message)
}
