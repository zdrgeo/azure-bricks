package dm

import (
	"github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message"
	"github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/event"
	"github.com/zdrgeo/azure-bricks/pubsub"
)

const (
	DiscriminatorDevice pubsub.Discriminator = "DM_Device"
)

const (
	StatusNone int = iota
	StatusOffline
	StatusOnline
)

type DeviceData struct {
	Code         string `json:"Code"`
	SerialNumber string `json:"SerialNumber"`
	TenantName   string `json:"TenantName"`
	Status       int    `json:"Status"`
}

type DeviceEvent struct {
	event.TenantGroupEvent
	Data *DeviceData `json:"Data"`
}

func NewDeviceEvent(version, operation, timestamp, tenantGroupName string, data *DeviceData) *DeviceEvent {
	return &DeviceEvent{
		TenantGroupEvent: event.TenantGroupEvent{
			Event: event.Event{
				Message: message.Message{
					Type: string(DiscriminatorDevice),
				},
				Version:   version,
				Operation: operation,
				Timestamp: timestamp,
			},
			TenantGroupName: tenantGroupName,
		},
		Data: data,
	}
}

func (message *DeviceEvent) Discriminator() pubsub.Discriminator {
	return DiscriminatorDevice
}
