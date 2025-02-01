package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/spf13/viper"
	"github.com/zdrgeo/azure-bricks/azure/servicebus/internal/azure/servicebus/message"
	"github.com/zdrgeo/azure-bricks/azure/servicebus/internal/handler/event/em"
	"github.com/zdrgeo/azure-bricks/azure/servicebus/pkg/azure/servicebus"
	"github.com/zdrgeo/azure-bricks/pubsub"
)

var (
	logger *slog.Logger

	credential *azidentity.DefaultAzureCredential
	client     *azservicebus.Client

	dispatcher *pubsub.Dispatcher
)

type partialMessage struct {
	Type string `json:"Type"`
}

func (message *partialMessage) Discriminator() pubsub.Discriminator {
	return pubsub.Discriminator(message.Type)
}

func init() {
	logger = slog.Default()
	// Use otelslog bridge to integrate with OpenTelemetry (https://pkg.go.dev/go.opentelemetry.io/otel/sdk/log)
	// logger := slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{AddSource: true}))
	// logger := slog.New(slog.NewJSONHandler(nil, &slog.HandlerOptions{AddSource: true}))

	viper.AddConfigPath(".")
	// viper.SetConfigFile(".env")
	// viper.SetConfigName("config")
	// viper.SetConfigType("env") // "env", "json", "yaml"
	viper.SetEnvPrefix("sync")
	viper.AutomaticEnv()

	viper.SetDefault("AZURE_SERVICEBUS_INTERVAL", 10*time.Second)
	viper.SetDefault("AZURE_SERVICEBUS_MESSAGES_LIMIT", 10)

	if err := viper.ReadInConfig(); err != nil {
		log.Panic(err)
	}

	var err error

	/*
		credential, err = azidentity.NewDefaultAzureCredential(nil)

		if err != nil {
			log.Panic(err)
		}

		client, err = azservicebus.NewClient(viper.GetString("AZURE_SERVICEBUS_NAMESPACE"), credential, nil)
	*/
	client, err = azservicebus.NewClientFromConnectionString(viper.GetString("AZURE_SERVICEBUS_CONNECTION_STRING"), nil)

	if err != nil {
		log.Panic(err)
	}

	dispatcher = pubsub.NewDispatcher()

	dispatcher.Register(em.NewPositionEventHandler(logger))
	dispatcher.Register(em.NewRoleEventHandler(logger))
	dispatcher.Register(em.NewEmployeeEventHandler(logger))
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

	subscriberOptions := &servicebus.SubscriberOptions{
		Interval:      viper.GetDuration("AZURE_SERVICEBUS_INTERVAL"),
		MessagesLimit: viper.GetInt("AZURE_SERVICEBUS_MESSAGES_LIMIT"),
	}

	subscriber := servicebus.NewSubscriber(receiver, dispatcher, message.NewUnmarshalReceivedEnvelopeFunc(message.NewUnmarshalMessageFunc(message.CreateMessage)), logger, subscriberOptions)

	if err := subscriber.Run(ctx); err != nil {
		log.Panic(err)
	}
}
