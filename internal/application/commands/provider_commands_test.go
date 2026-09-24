package commands

import (
	"context"
	"errors"
	"testing"

	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
)

func TestProviderCommandsCreateProviderEncryptsCredentialsBeforePersistence(t *testing.T) {
	t.Parallel()

	repository := &providerCommandRepository{}
	cipher := &providerCommandCipher{ciphertext: "v1.primary.nonce.ciphertext"}
	commands := NewProviderCommands(ProviderCommandsParams{ProviderRepository: repository, CredentialsCipher: cipher})
	credentials := []byte(`{"project_id":"example-project"}`)
	result, err := commands.CreateProvider(context.Background(), application.CreateProviderCommand{
		TenantID: uuid.NewV7(), Type: domain.ProviderTypeFCM, Credentials: credentials, RateLimitQPS: 100, RateLimitBurst: 100,
	})
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}
	if result.ProviderID == uuid.Nil() {
		t.Fatal("expected provider ID")
	}
	if string(cipher.plaintext) != string(credentials) {
		t.Fatalf("plaintext = %q, want %q", cipher.plaintext, credentials)
	}
	if repository.provider == nil || repository.provider.EncryptedCredentials() != cipher.ciphertext {
		t.Fatalf("persisted provider = %+v", repository.provider)
	}
}

func TestProviderCommandsCreateProviderRejectsEmptyOrUnencryptableCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		command application.CreateProviderCommand
		cipher  *providerCommandCipher
		want    error
	}{
		{
			name:    "empty credentials",
			command: application.CreateProviderCommand{TenantID: uuid.NewV7(), Type: domain.ProviderTypeFCM, RateLimitQPS: 100, RateLimitBurst: 100},
			cipher:  &providerCommandCipher{},
			want:    application.ErrValidation,
		},
		{
			name:    "cipher unavailable",
			command: application.CreateProviderCommand{TenantID: uuid.NewV7(), Type: domain.ProviderTypeFCM, Credentials: []byte("credentials"), RateLimitQPS: 100, RateLimitBurst: 100},
			cipher:  &providerCommandCipher{err: errors.New("key unavailable")},
			want:    application.ErrUnavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := &providerCommandRepository{}
			commands := NewProviderCommands(ProviderCommandsParams{ProviderRepository: repository, CredentialsCipher: test.cipher})
			if _, err := commands.CreateProvider(context.Background(), test.command); !errors.Is(err, test.want) {
				t.Fatalf("create provider error = %v, want %v", err, test.want)
			}
			if repository.provider != nil {
				t.Fatal("provider must not be persisted")
			}
		})
	}
}

type providerCommandRepository struct {
	provider *domain.Provider
}

func (r *providerCommandRepository) Create(_ context.Context, provider *domain.Provider) error {
	r.provider = provider
	return nil
}

func (r *providerCommandRepository) FindByID(context.Context, domain.ProviderID) (*domain.Provider, error) {
	return r.provider, nil
}

type providerCommandCipher struct {
	plaintext  []byte
	ciphertext string
	err        error
}

func (c *providerCommandCipher) Encrypt(_ context.Context, plaintext []byte) (string, error) {
	c.plaintext = append([]byte(nil), plaintext...)
	if c.err != nil {
		return "", c.err
	}
	return c.ciphertext, nil
}

func (c *providerCommandCipher) Decrypt(context.Context, string) ([]byte, error) { return nil, nil }
