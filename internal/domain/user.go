package domain

import (
	"fmt"
	"maps"
	"strings"
)

type UserStatus string

const (
	UserStatusActive  UserStatus = "active"
	UserStatusDeleted UserStatus = "deleted"
)

type NewUserParams struct {
	TenantID   TenantID
	UserID     UserID
	Attributes map[string]string
}

// User is Pushkin's tenant-scoped local representation of an externally owned
// user. It provides only attributes explicitly replicated for product rules.
type User struct {
	tenantID   TenantID
	id         UserID
	attributes map[string]string
	status     UserStatus
}

func NewUser(params NewUserParams) (*User, error) {
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return nil, err
	}
	if err := requireIdentifier("user_id", string(params.UserID)); err != nil {
		return nil, err
	}
	for key := range params.Attributes {
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("%w: user attribute key must not be empty", ErrInvalidArgument)
		}
	}
	return &User{tenantID: params.TenantID, id: params.UserID, attributes: maps.Clone(params.Attributes), status: UserStatusActive}, nil
}

func (u *User) Delete()                       { u.status = UserStatusDeleted }
func (u *User) TenantID() TenantID            { return u.tenantID }
func (u *User) ID() UserID                    { return u.id }
func (u *User) Status() UserStatus            { return u.status }
func (u *User) Attributes() map[string]string { return maps.Clone(u.attributes) }
