package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
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
}

func main() {
	notifyContext, cancelNotify := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelNotify()

	receiver, err := client.NewReceiverForSubscription(viper.GetString("AZURE_SERVICEBUS_TOPIC"), viper.GetString("AZURE_SERVICEBUS_SUBSCRIPTION"), nil)
	if err != nil {
		log.Panic(err)
	}

	defer receiver.Close(notifyContext)

	tick := time.Tick(1 * time.Minute)

	for done := false; !done; {
		select {
		case <-notifyContext.Done():
			done = true
		case <-tick:
			messages, err := receiver.ReceiveMessages(notifyContext, viper.GetInt("AZURE_SERVICEBUS_MESSAGE_LIMIT"), nil)
			if err != nil {
				log.Panic(err)
			}

			for _, message := range messages {
				if err := handleMessage(message); err != nil {
					if err := receiver.AbandonMessage(notifyContext, message, nil); err != nil {
						log.Panic(err)
					}
				}

				if err := receiver.CompleteMessage(notifyContext, message, nil); err != nil {
					log.Panic(err)
				}
			}
		}
	}
}

func sessionMain() {
	notifyContext, cancelNotify := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelNotify()

	sessions := viper.GetStringSlice("AZURE_SERVICEBUS_SESSION")

	wg := sync.WaitGroup{}

	for _, session := range sessions {
		wg.Add(1)

		go func() {
			defer wg.Done()

			sessionReceiver, err := client.AcceptSessionForSubscription(notifyContext, viper.GetString("AZURE_SERVICEBUS_TOPIC"), viper.GetString("AZURE_SERVICEBUS_SUBSCRIPTION"), session, nil)

			if err != nil {
				log.Panic(err)
			}

			defer sessionReceiver.Close(notifyContext)

			tick := time.Tick(1 * time.Minute)

			for done := false; !done; {
				select {
				case <-notifyContext.Done():
					done = true
				case <-tick:
					messages, err := sessionReceiver.ReceiveMessages(notifyContext, viper.GetInt("AZURE_SERVICEBUS_MESSAGE_LIMIT"), nil)
					if err != nil {
						log.Panic(err)
					}

					for _, message := range messages {
						if err := handleMessage(message); err != nil {
							if err := sessionReceiver.AbandonMessage(notifyContext, message, nil); err != nil {
								log.Panic(err)
							}
						}

						if err := sessionReceiver.CompleteMessage(notifyContext, message, nil); err != nil {
							log.Panic(err)
						}
					}
				}
			}
		}()
	}

	wg.Wait()
}

func nextSessionMain() {
	notifyContext, cancelNotify := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelNotify()

	wg := sync.WaitGroup{}

	sessionLimit := make(chan struct{}, viper.GetInt("AZURE_SERVICEBUS_SESSION_LIMIT"))

	for sessionDone := false; !sessionDone; {
		select {
		case <-notifyContext.Done():
			sessionDone = true
		case sessionLimit <- struct{}{}:
			wg.Add(1)

			go func() {
				defer func() {
					wg.Done()

					<-sessionLimit
				}()

				sessionReceiver, err := client.AcceptNextSessionForSubscription(notifyContext, viper.GetString("AZURE_SERVICEBUS_TOPIC"), viper.GetString("AZURE_SERVICEBUS_SUBSCRIPTION"), nil)

				if err != nil {
					var serviceBusErr *azservicebus.Error

					if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeTimeout {
						return
					}

					log.Panic(err)
				}

				defer sessionReceiver.Close(notifyContext)

				tick := time.Tick(1 * time.Minute)

				for done := false; !done; {
					select {
					case <-notifyContext.Done():
						done = true
					case <-tick:
						messages, err := sessionReceiver.ReceiveMessages(notifyContext, viper.GetInt("AZURE_SERVICEBUS_MESSAGE_LIMIT"), nil)
						if err != nil {
							log.Panic(err)
						}

						for _, message := range messages {
							if err := handleMessage(message); err != nil {
								if err := sessionReceiver.AbandonMessage(notifyContext, message, nil); err != nil {
									log.Panic(err)
								}
							}

							if err := sessionReceiver.CompleteMessage(notifyContext, message, nil); err != nil {
								log.Panic(err)
							}
						}
					}
				}
			}()
		}
	}

	wg.Wait()
}

func handleMessage(message *azservicebus.ReceivedMessage) error {
	return handleMessageBody(message.Body)
}

func handleMessageBody(messageBody []byte) error {
	_ = messageBody
	return nil
}
