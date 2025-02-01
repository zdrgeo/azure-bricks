package em

import (
	"github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message"
	"github.com/zdrgeo/azure-bricks/azure/servicebus/internal/message/event"
	"github.com/zdrgeo/azure-bricks/pubsub"
)

const (
	DiscriminatorEmployee pubsub.Discriminator = "EM_Employee"
	DiscriminatorPosition pubsub.Discriminator = "EM_Position"
	DiscriminatorRole     pubsub.Discriminator = "EM_Role"
)

const (
	ScopeNone int = iota
	ScopeTenantGroup
	ScopePartnerGroup
)

const (
	StatusNone int = iota
	StatusActive
	StatusInactive
	StatusLeft
)

const (
	QualificationNone int = iota
	QualificationCertified
)

// Flags for ContactTypes
const ContactTypesNone int = 0
const (
	ContactTypesPrimaryEscalationPoint int = 1 << iota
	ContactTypesSecondaryEscalationPoint
)

type TenantModel struct {
	Name         string   `json:"Name"`
	CircleCodes  []string `json:"CircleCodes"`
	ZoneCodes    []string `json:"ZoneCodes"`
	StorageCodes []string `json:"StorageCodes"`
}

type EmployeeTenantGroupModel struct {
	Tenants []*TenantModel `json:"Tenants"`
}

type EmployeePartnerGroupModel struct {
	TenantName       string   `json:"TenantName"`
	PartnerGroupCode string   `json:"PartnerGroupCode"`
	PartnerGroupName string   `json:"PartnerGroupName"`
	Qualification    int      `json:"Qualification"`
	ContactTypes     int      `json:"ContactTypes"`
	PartnerCodes     []string `json:"PartnerCodes"`
}

type EmployeeRelationModel struct {
	Code     string `json:"Code"`
	UserId   string `json:"UserId"`
	UserName string `json:"UserName"`
	Scope    int    `json:"Scope"`
}

type EmployeeData struct {
	Code               string                     `json:"Code"`
	UserId             string                     `json:"UserId"`
	UserName           string                     `json:"UserName"`
	Scope              int                        `json:"Scope"`
	FirstName          string                     `json:"FirstName"`
	MiddleName         string                     `json:"MiddleName"`
	LastName           string                     `json:"LastName"`
	PrimaryPhone       string                     `json:"PrimaryPhone"`
	SecondaryPhone     string                     `json:"SecondaryPhone"`
	Email              string                     `json:"Email"`
	Status             int                        `json:"Status"`
	PositionCode       string                     `json:"PositionCode"`
	PositionName       string                     `json:"PositionName"`
	ReportsToEmployees []*EmployeeRelationModel   `json:"ReportsToEmployees"`
	SystemCodes        []string                   `json:"SystemCodes"`
	RoleCodes          []string                   `json:"RoleCodes"`
	TenantGroup        *EmployeeTenantGroupModel  `json:"TenantGroup"`
	PartnerGroup       *EmployeePartnerGroupModel `json:"PartnerGroup"`
	CreatedByUserId    string                     `json:"CreatedByUserId"`
	CreatedByUserName  string                     `json:"CreatedByUserName"`
	ModifiedByUserId   string                     `json:"ModifiedByUserId"`
	ModifiedByUserName string                     `json:"ModifiedByUserName"`
}

type EmployeeEvent struct {
	event.TenantGroupEvent
	Data *EmployeeData `json:"Data"`
}

func NewEmployeeEvent(version, operation, timestamp, tenantGroupName string, data *EmployeeData) *EmployeeEvent {
	return &EmployeeEvent{
		TenantGroupEvent: event.TenantGroupEvent{
			Event: event.Event{
				Message: message.Message{
					Type: string(DiscriminatorEmployee),
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

func (message *EmployeeEvent) Discriminator() pubsub.Discriminator {
	return DiscriminatorEmployee
}

type PositionData struct {
	Code          string `json:"Code"`
	Name          string `json:"Name"`
	Scope         int    `json:"Scope"`
	ReportsToCode string `json:"ReportsToCode"`
}

type PositionEvent struct {
	event.TenantGroupEvent
	Data *PositionData `json:"Data"`
}

func NewPositionEvent(version, operation, timestamp, tenantGroupName string, data *PositionData) *PositionEvent {
	return &PositionEvent{
		TenantGroupEvent: event.TenantGroupEvent{
			Event: event.Event{
				Message: message.Message{
					Type: string(DiscriminatorPosition),
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

func (message *PositionEvent) Discriminator() pubsub.Discriminator {
	return DiscriminatorPosition
}

type RoleData struct {
	Code        string   `json:"Code"`
	Name        string   `json:"Name"`
	Scope       int      `json:"Scope"`
	Permissions []string `json:"Permissions"`
}

type RoleEvent struct {
	event.TenantGroupEvent
	Data *RoleData `json:"Data"`
}

func NewRoleEvent(version, operation, timestamp, tenantGroupName string, data *RoleData) *RoleEvent {
	return &RoleEvent{
		TenantGroupEvent: event.TenantGroupEvent{
			Event: event.Event{
				Message: message.Message{
					Type: string(DiscriminatorRole),
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

func (message *RoleEvent) Discriminator() pubsub.Discriminator {
	return DiscriminatorRole
}
