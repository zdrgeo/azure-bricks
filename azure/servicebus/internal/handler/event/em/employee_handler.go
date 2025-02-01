package em

import (
	"encoding/json"
	"fmt"
	"log/slog"

	envelopemessage "github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/envelope"
	hrevent "github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/event/em"
	"github.com/zdrgeo/azure-bricks/pubsub"
)

type EmployeeEventHandler struct {
	logger *slog.Logger
}

func NewEmployeeEventHandler(logger *slog.Logger) *EmployeeEventHandler {
	return &EmployeeEventHandler{
		logger: logger,
	}
}

func (handler *EmployeeEventHandler) Discriminator() pubsub.Discriminator {
	return hrevent.DiscriminatorEmployee
}

func (handler *EmployeeEventHandler) Handle(message pubsub.Message) error {
	receivedEnvelope, ok := message.(*envelopemessage.ReceivedEnvelope)

	if !ok {
		return envelopemessage.ErrInvalidReceivedEnvelope
	}

	employeeEvent, ok := receivedEnvelope.Message.(*hrevent.EmployeeEvent)

	if !ok {
		return pubsub.ErrInvalidDiscriminator
	}

	// Replace with your own synchronization logic
	data, err := json.MarshalIndent(employeeEvent, "", "  ")

	if err != nil {
		return err
	}

	fmt.Println(string(data))
	// Replace with your own synchronization logic

	return nil
}
