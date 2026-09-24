package credentials

import (
	"bytes"
	"context"
	"testing"
)

func TestCipherRoundTripUsesDistinctEnvelopes(t *testing.T) {
	t.Parallel()

	cipher := newTestCipher(t)
	plaintext := []byte(`{"project_id":"example"}`)
	first, err := cipher.Encrypt(context.Background(), plaintext)
	if err != nil {
		t.Fatalf("encrypt first: %v", err)
	}
	second, err := cipher.Encrypt(context.Background(), plaintext)
	if err != nil {
		t.Fatalf("encrypt second: %v", err)
	}
	if first == second {
		t.Fatal("independent encryption must use different nonces")
	}
	decrypted, err := cipher.Decrypt(context.Background(), first)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("plaintext = %q, want %q", decrypted, plaintext)
	}
}

func TestCipherRejectsTampering(t *testing.T) {
	t.Parallel()

	cipher := newTestCipher(t)
	envelope, err := cipher.Encrypt(context.Background(), []byte("credentials"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := cipher.Decrypt(context.Background(), envelope[:len(envelope)-1]+"x"); err == nil {
		t.Fatal("expected tampered envelope error")
	}
}

func TestNewCipherRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	if _, err := NewCipher(CipherParams{Key: make([]byte, 31)}); err == nil {
		t.Fatal("expected invalid key length error")
	}
}

func newTestCipher(t *testing.T) *Cipher {
	t.Helper()
	cipher, err := NewCipher(CipherParams{Key: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	return cipher
}
