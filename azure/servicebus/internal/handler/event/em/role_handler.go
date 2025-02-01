package em

import (
	"encoding/json"
	"fmt"
	"log/slog"

	envelopemessage "github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/envelope"
	emevent "github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/event/em"
	"github.com/zdrgeo/azure-bricks/pubsub"
)

type RoleEventHandler struct {
	logger *slog.Logger
}

func NewRoleEventHandler(logger *slog.Logger) *RoleEventHandler {
	return &RoleEventHandler{
		logger: logger,
	}
}

func (handler *RoleEventHandler) Discriminator() pubsub.Discriminator {
	return emevent.DiscriminatorRole
}

func (handler *RoleEventHandler) Handle(message pubsub.Message) error {
	receivedEnvelope, ok := message.(*envelopemessage.ReceivedEnvelope)

	if !ok {
		return envelopemessage.ErrInvalidReceivedEnvelope
	}

	roleEvent, ok := receivedEnvelope.Message.(*emevent.RoleEvent)

	if !ok {
		return pubsub.ErrInvalidDiscriminator
	}

	// Replace with your own synchronization logic
	data, err := json.MarshalIndent(roleEvent, "", "  ")

	if err != nil {
		return err
	}

	fmt.Println(string(data))
	// Replace with your own synchronization logic

	return nil
}
