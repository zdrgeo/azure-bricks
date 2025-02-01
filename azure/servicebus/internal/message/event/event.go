package event

import (
	"github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message"
)

const (
	Version1 string = "1"
)

const (
	OperationAdd      string = "Add"
	OperationAddOrSet string = "AddOrSet"
	OperationRemove   string = "Remove"
)

type Event struct {
	message.Message
	Version   string `json:"Version"`
	Operation string `json:"Operation"`
	Timestamp string `json:"Timestamp"`
}

type TenantGroupEvent struct {
	Event
	TenantGroupName string `json:"TenantGroupName"`
}

type TenantEvent struct {
	TenantGroupEvent
	TenantName string `json:"TenantName"`
}
