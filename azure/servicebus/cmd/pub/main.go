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
	envelopemessage "github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/envelope"
	"github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/event"
	"github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/event/dm"
	"github.com/zdrgeo/azure-bricks/azure/servicebus/pkg/azure/servicebus"
)

const (
	TenantGroupNameExcitel string = "excitel"
)

const (
	TenantNameDelhi     string = "delhi"
	TenantNameHyderabad string = "hyderabad"
	TenantNameMumbai    string = "mumbai"
)

var (
	logger *slog.Logger

	credential *azidentity.DefaultAzureCredential
	client     *azservicebus.Client
)

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
}

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelCtx()

	sender, err := client.NewSender(viper.GetString("AZURE_SERVICEBUS_TOPIC"), nil)

	if err != nil {
		log.Panic(err)
	}

	defer sender.Close(ctx)

	publisher := servicebus.NewPublisher(sender, message.NewMarshalEnvelopeFunc(message.NewMarshalMessageFunc()), logger, nil)

	tick := time.Tick(10 * time.Second)

	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick:
			message := dm.NewDeviceEvent(event.Version1, event.OperationAddOrSet, time.Now().UTC().Format(time.RFC3339), TenantGroupNameExcitel,
				&dm.DeviceData{
					Code:         "1234",
					SerialNumber: "1234",
					TenantName:   TenantNameDelhi,
					Status:       dm.StatusOnline,
				},
			)

			envelope := envelopemessage.NewEnvelope(message)

			envelope.ApplicationProperties = map[string]any{"Event": true, "Type": "DM_Device", "TenantGroupName": TenantGroupNameExcitel, TenantNameDelhi: "TenantName"}

			if err := publisher.Publish(ctx, envelope); err != nil {
				log.Panic(err)
			}
		}
	}
}
