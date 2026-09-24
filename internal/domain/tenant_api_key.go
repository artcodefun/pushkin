package domain

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"uuid"
)

// TenantAPIKey is an issued credential. Its plaintext secret is never kept in
// domain state or persistence; only its keyed hash is durable.
type TenantAPIKey struct {
	id         uuid.UUID
	tenantID   TenantID
	name       string
	secretHash []byte
}

type NewTenantAPIKeyParams struct {
	TenantID   TenantID
	Name       string
	SecretHash []byte
}

func NewTenantAPIKey(params NewTenantAPIKeyParams) (*TenantAPIKey, error) {
	if err := requireUUID("tenant_id", params.TenantID); err != nil {
		return nil, err
	}
	if strings.TrimSpace(params.Name) == "" {
		return nil, fmt.Errorf("%w: API key name must not be empty", ErrInvalidArgument)
	}
	if len(params.SecretHash) != sha256.Size {
		return nil, fmt.Errorf("%w: API key secret hash must be %d bytes", ErrInvalidArgument, sha256.Size)
	}
	return &TenantAPIKey{
		id:         uuid.NewV7(),
		tenantID:   params.TenantID,
		name:       params.Name,
		secretHash: append([]byte(nil), params.SecretHash...),
	}, nil
}

func (k *TenantAPIKey) ID() uuid.UUID      { return k.id }
func (k *TenantAPIKey) TenantID() TenantID { return k.tenantID }
func (k *TenantAPIKey) Name() string       { return k.name }
func (k *TenantAPIKey) SecretHash() []byte { return append([]byte(nil), k.secretHash...) }

type HydrateTenantAPIKeyParams struct {
	ID         uuid.UUID
	TenantID   TenantID
	Name       string
	SecretHash []byte
}

func HydrateTenantAPIKey(params HydrateTenantAPIKeyParams) (*TenantAPIKey, error) {
	if err := requireUUID("api_key_id", params.ID); err != nil {
		return nil, err
	}
	key, err := NewTenantAPIKey(NewTenantAPIKeyParams{
		TenantID:   params.TenantID,
		Name:       params.Name,
		SecretHash: params.SecretHash,
	})
	if err != nil {
		return nil, err
	}
	key.id = params.ID
	return key, nil
}
