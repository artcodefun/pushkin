package security

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"

	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

var _ ports.TenantAPIKeyHasher = (*TenantAPIKeyHasher)(nil)
var _ ports.TenantAPIKeyValidator = (*TenantAPIKeyValidator)(nil)

type TenantAPIKeyHasher struct {
	pepper []byte
}

func NewTenantAPIKeyHasher(pepper []byte) (*TenantAPIKeyHasher, error) {
	if len(pepper) == 0 {
		return nil, fmt.Errorf("tenant API key hash pepper must not be empty")
	}
	return &TenantAPIKeyHasher{pepper: append([]byte(nil), pepper...)}, nil
}

func (h *TenantAPIKeyHasher) Hash(_ context.Context, secret []byte) ([]byte, error) {
	if len(secret) == 0 {
		return nil, fmt.Errorf("tenant API key secret must not be empty")
	}
	mac := hmac.New(sha256.New, h.pepper)
	_, _ = mac.Write(secret)
	return mac.Sum(nil), nil
}

type TenantAPIKeyValidator struct {
	repository ports.TenantAPIKeyRepository
	hasher     ports.TenantAPIKeyHasher
}

func NewTenantAPIKeyValidator(
	repository ports.TenantAPIKeyRepository,
	hasher ports.TenantAPIKeyHasher,
) *TenantAPIKeyValidator {
	return &TenantAPIKeyValidator{repository: repository, hasher: hasher}
}

func (v *TenantAPIKeyValidator) Validate(ctx context.Context, rawKey string) (domain.TenantID, error) {
	id, secret, err := parseTenantAPIKey(rawKey)
	if err != nil {
		return uuid.Nil(), application.ErrNotAuthorized
	}
	key, err := v.repository.FindActiveByID(ctx, id)
	if err != nil {
		return uuid.Nil(), err
	}
	if key == nil {
		return uuid.Nil(), application.ErrNotAuthorized
	}
	hash, err := v.hasher.Hash(ctx, secret)
	if err != nil || !hmac.Equal(hash, key.SecretHash()) {
		return uuid.Nil(), application.ErrNotAuthorized
	}
	return key.TenantID(), nil
}

func parseTenantAPIKey(rawKey string) (domain.TenantAPIKeyID, []byte, error) {
	parts := strings.SplitN(rawKey, "_", 3)
	if len(parts) != 3 || parts[0] != "pk" {
		return uuid.Nil(), nil, fmt.Errorf("invalid tenant API key format")
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return uuid.Nil(), nil, fmt.Errorf("parse tenant API key ID: %w", err)
	}
	secret, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(secret) == 0 {
		return uuid.Nil(), nil, fmt.Errorf("decode tenant API key secret")
	}
	return id, secret, nil
}
