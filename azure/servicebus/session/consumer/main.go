package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/spf13/viper"
	"github.com/zdrgeo/azure-bricks/azure-servicebus-session-consumer/pkg/processor"
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

	viper.SetDefault("AZURE_SERVICEBUS_SESSIONS_LIMIT", 10)
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

	sessions := viper.GetStringSlice("AZURE_SERVICEBUS_SESSIONS")

	sessionsGroup := sync.WaitGroup{}

	for _, session := range sessions {
		sessionsGroup.Add(1)

		go func() {
			defer sessionsGroup.Done()

			sessionReceiverOptions := &azservicebus.SessionReceiverOptions{
				ReceiveMode: azservicebus.ReceiveModePeekLock,
			}

			sessionReceiver, err := client.AcceptSessionForSubscription(ctx, viper.GetString("AZURE_SERVICEBUS_TOPIC"), viper.GetString("AZURE_SERVICEBUS_SUBSCRIPTION"), session, sessionReceiverOptions)

			if err != nil {
				log.Panic(err)
			}

			defer sessionReceiver.Close(ctx)

			tick := time.Tick(10 * time.Second)

			for done := false; !done; {
				select {
				case <-ctx.Done():
					done = true
				case <-tick:
					serviceBusReceivedMessages, err := sessionReceiver.ReceiveMessages(ctx, viper.GetInt("AZURE_SERVICEBUS_MESSAGES_LIMIT"), nil)

					if err != nil {
						log.Panic(err)
					}

					for _, serviceBusReceivedMessage := range serviceBusReceivedMessages {
						discriminator, err := processor.UnmarshalDiscriminator(serviceBusReceivedMessage.Body)

						if err != nil {
							deadLetterOptions := &azservicebus.DeadLetterOptions{
								ErrorDescription: to.Ptr(err.Error()),
								Reason:           to.Ptr("UnmarshalDiscriminatorError"),
							}

							if err := sessionReceiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
								var serviceBusErr *azservicebus.Error

								if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
									continue
								}

								log.Panic(err)
							}
						}

						if handler, ok := dispatcher.Dispatch(discriminator); ok {
							message := handler.Create()

							if err := processor.UnmarshalMessage(serviceBusReceivedMessage.Body, message); err != nil {
								deadLetterOptions := &azservicebus.DeadLetterOptions{
									ErrorDescription: to.Ptr(err.Error()),
									Reason:           to.Ptr("UnmarshalMessageError"),
								}

								if err := sessionReceiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
									var serviceBusErr *azservicebus.Error

									if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
										continue
									}

									log.Panic(err)
								}
							}

							if err := handler.Handle(message); err != nil {
								if err := sessionReceiver.AbandonMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
									var serviceBusErr *azservicebus.Error

									if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
										continue
									}

									log.Panic(err)
								}
							}
						}

						if err := sessionReceiver.CompleteMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
							var serviceBusErr *azservicebus.Error

							if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
								continue
							}

							log.Panic(err)
						}
					}
				}
			}
		}()
	}

	sessionsGroup.Wait()
}

func nextMain() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt)

	defer cancelCtx()

	sessionsGroup := sync.WaitGroup{}

	sessionsLimit := make(chan struct{}, viper.GetInt("AZURE_SERVICEBUS_SESSIONS_LIMIT"))

	for sessionDone := false; !sessionDone; {
		select {
		case <-ctx.Done():
			sessionDone = true
		case sessionsLimit <- struct{}{}:
			sessionsGroup.Add(1)

			go func() {
				defer func() {
					sessionsGroup.Done()

					<-sessionsLimit
				}()

				sessionReceiverOptions := &azservicebus.SessionReceiverOptions{
					ReceiveMode: azservicebus.ReceiveModePeekLock,
				}

				sessionReceiver, err := client.AcceptNextSessionForSubscription(ctx, viper.GetString("AZURE_SERVICEBUS_TOPIC"), viper.GetString("AZURE_SERVICEBUS_SUBSCRIPTION"), sessionReceiverOptions)

				if err != nil {
					var serviceBusErr *azservicebus.Error

					if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeTimeout {
						return
					}

					log.Panic(err)
				}

				defer sessionReceiver.Close(ctx)

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 10*time.Minute)

				defer timeoutCancel()

				tick := time.Tick(10 * time.Second)

				for done := false; !done; {
					select {
					case <-ctx.Done():
						done = true
					case <-tick:
						serviceBusReceivedMessages, err := sessionReceiver.ReceiveMessages(timeoutCtx, viper.GetInt("AZURE_SERVICEBUS_MESSAGES_LIMIT"), nil)

						if err != nil {
							log.Panic(err)
						}

						for _, serviceBusReceivedMessage := range serviceBusReceivedMessages {
							discriminator, err := processor.UnmarshalDiscriminator(serviceBusReceivedMessage.Body)

							if err != nil {
								deadLetterOptions := &azservicebus.DeadLetterOptions{
									ErrorDescription: to.Ptr(err.Error()),
									Reason:           to.Ptr("UnmarshalDiscriminatorError"),
								}

								if err := sessionReceiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
									var serviceBusErr *azservicebus.Error

									if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
										continue
									}

									log.Panic(err)
								}
							}

							if handler, ok := dispatcher.Dispatch(discriminator); ok {
								message := handler.Create()

								if err := processor.UnmarshalMessage(serviceBusReceivedMessage.Body, message); err != nil {
									deadLetterOptions := &azservicebus.DeadLetterOptions{
										ErrorDescription: to.Ptr(err.Error()),
										Reason:           to.Ptr("UnmarshalMessageError"),
									}

									if err := sessionReceiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
										var serviceBusErr *azservicebus.Error

										if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
											continue
										}

										log.Panic(err)
									}
								}

								if err := handler.Handle(message); err != nil {
									if err := sessionReceiver.AbandonMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
										var serviceBusErr *azservicebus.Error

										if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
											continue
										}

										log.Panic(err)
									}
								}
							}

							if err := sessionReceiver.CompleteMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
								var serviceBusErr *azservicebus.Error

								if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
									continue
								}

								log.Panic(err)
							}
						}
					}
				}
			}()
		}
	}

	sessionsGroup.Wait()
}

type Session struct {
	SessionReceiver *azservicebus.SessionReceiver
}

func newSessionProducer(client *azservicebus.Client) (producerFunc processor.ProducerFunc[*Session, any], producerData any) {
	return func(ctx context.Context, data any) (item *Session, foldData any, err error) {
		for {
			sessionReceiverOptions := &azservicebus.SessionReceiverOptions{
				ReceiveMode: azservicebus.ReceiveModePeekLock,
			}

			sessionReceiver, err := client.AcceptNextSessionForSubscription(ctx, viper.GetString("AZURE_SERVICEBUS_TOPIC"), viper.GetString("AZURE_SERVICEBUS_SUBSCRIPTION"), sessionReceiverOptions)

			if err != nil {
				var serviceBusErr *azservicebus.Error

				if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeTimeout {
					continue
				}

				return nil, nil, err
			}

			session := &Session{
				SessionReceiver: sessionReceiver,
			}

			return session, nil, nil
		}
	}, nil
}

func newSessionConsumer(dispatcher *processor.Dispatcher) (consumerFunc processor.ConsumerFunc[*Session, any], consumerData any) {
	return func(ctx context.Context, item *Session, data any) (foldData any, err error) {
		defer item.SessionReceiver.Close(ctx)

		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 10*time.Minute)

		defer timeoutCancel()

		tick := time.Tick(10 * time.Second)

		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-tick:
				serviceBusReceivedMessages, err := item.SessionReceiver.ReceiveMessages(timeoutCtx, viper.GetInt("AZURE_SERVICEBUS_MESSAGES_LIMIT"), nil)

				if err != nil {
					return nil, err
				}

				for _, serviceBusReceivedMessage := range serviceBusReceivedMessages {
					discriminator, err := processor.UnmarshalDiscriminator(serviceBusReceivedMessage.Body)

					if err != nil {
						deadLetterOptions := &azservicebus.DeadLetterOptions{
							ErrorDescription: to.Ptr(err.Error()),
							Reason:           to.Ptr("UnmarshalDiscriminatorError"),
						}

						if err := item.SessionReceiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
							var serviceBusErr *azservicebus.Error

							if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
								continue
							}

							return nil, err
						}
					}

					if handler, ok := dispatcher.Dispatch(discriminator); ok {
						message := handler.Create()

						if err := processor.UnmarshalMessage(serviceBusReceivedMessage.Body, message); err != nil {
							deadLetterOptions := &azservicebus.DeadLetterOptions{
								ErrorDescription: to.Ptr(err.Error()),
								Reason:           to.Ptr("UnmarshalMessageError"),
							}

							if err := item.SessionReceiver.DeadLetterMessage(ctx, serviceBusReceivedMessage, deadLetterOptions); err != nil {
								var serviceBusErr *azservicebus.Error

								if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
									continue
								}

								return nil, err
							}
						}

						if err := handler.Handle(message); err != nil {
							if err := item.SessionReceiver.AbandonMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
								var serviceBusErr *azservicebus.Error

								if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
									continue
								}

								return nil, err
							}
						}
					}

					if err := item.SessionReceiver.CompleteMessage(ctx, serviceBusReceivedMessage, nil); err != nil {
						var serviceBusErr *azservicebus.Error

						if errors.As(err, &serviceBusErr) && serviceBusErr.Code == azservicebus.CodeLockLost {
							continue
						}

						return nil, err
					}
				}
			}
		}
	}, nil
}

func processSessions(ctx context.Context, client *azservicebus.Client) {
	producerFunc, _ := newSessionProducer(client)

	consumerFunc, _ := newSessionConsumer(dispatcher)

	processor.Run(ctx, 10, []processor.ProducerFunc[*Session, any]{producerFunc}, []processor.ConsumerFunc[*Session, any]{consumerFunc})
}
