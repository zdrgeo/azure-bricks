package em

import (
	"encoding/json"
	"fmt"
	"log/slog"

	envelopemessage "github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/envelope"
	emevent "github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/event/em"
	"github.com/zdrgeo/azure-bricks/pubsub"
)

type PositionEventHandler struct {
	logger *slog.Logger
}

func NewPositionEventHandler(logger *slog.Logger) *PositionEventHandler {
	return &PositionEventHandler{
		logger: logger,
	}
}

func (handler *PositionEventHandler) Discriminator() pubsub.Discriminator {
	return emevent.DiscriminatorPosition
}

func (handler *PositionEventHandler) Handle(message pubsub.Message) error {
	receivedEnvelope, ok := message.(*envelopemessage.ReceivedEnvelope)

	if !ok {
		return envelopemessage.ErrInvalidReceivedEnvelope
	}

	positionEvent, ok := receivedEnvelope.Message.(*emevent.PositionEvent)

	if !ok {
		return pubsub.ErrInvalidDiscriminator
	}

	// Replace with your own synchronization logic
	data, err := json.MarshalIndent(positionEvent, "", "  ")

	if err != nil {
		return err
	}

	fmt.Println(string(data))
	// Replace with your own synchronization logic

	return nil
}
