package ports

import "context"

// CredentialsCipher protects provider credentials before they are persisted.
// Plaintext credentials must not be returned by application queries or logged.
type CredentialsCipher interface {
	Encrypt(ctx context.Context, plaintext []byte) (ciphertext string, err error)
	Decrypt(ctx context.Context, ciphertext string) (plaintext []byte, err error)
}
