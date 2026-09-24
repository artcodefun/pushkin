package credentials

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/superman/pushkin/internal/application/ports"
)

var _ ports.CredentialsCipher = (*Cipher)(nil)

const envelopeVersion = "v1"

type CipherParams struct {
	Key []byte
}

// Cipher encrypts provider credentials using AES-256-GCM. The resulting value
// is a self-contained versioned envelope suitable for a PostgreSQL TEXT column.
type Cipher struct {
	aead cipher.AEAD
}

func NewCipher(params CipherParams) (*Cipher, error) {
	if len(params.Key) != 32 {
		return nil, fmt.Errorf("credentials cipher key must be exactly 32 bytes")
	}
	block, err := aes.NewCipher(params.Key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create AES-GCM: %w", err)
	}
	return &Cipher{aead: aead}, nil
}

func (c *Cipher) Encrypt(_ context.Context, plaintext []byte) (string, error) {
	if len(plaintext) == 0 {
		return "", fmt.Errorf("credentials plaintext must not be empty")
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate credentials nonce: %w", err)
	}
	ciphertext := c.aead.Seal(nil, nonce, plaintext, nil)
	return strings.Join([]string{
		envelopeVersion,
		base64.RawURLEncoding.EncodeToString(nonce),
		base64.RawURLEncoding.EncodeToString(ciphertext),
	}, "."), nil
}

func (c *Cipher) Decrypt(_ context.Context, envelope string) ([]byte, error) {
	parts := strings.Split(envelope, ".")
	if len(parts) != 3 || parts[0] != envelopeVersion {
		return nil, fmt.Errorf("invalid credentials cipher envelope")
	}
	nonce, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode credentials nonce: %w", err)
	}
	if len(nonce) != c.aead.NonceSize() {
		return nil, fmt.Errorf("invalid credentials nonce size")
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("decode credentials ciphertext: %w", err)
	}
	plaintext, err := c.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt credentials: %w", err)
	}
	return plaintext, nil
}
