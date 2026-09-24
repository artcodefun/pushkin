package v1

import "testing"

func TestEncodeAndDecodeMessage(t *testing.T) {
	t.Parallel()

	want := UserEventV1{Type: UserEventTypeCreated, TenantID: "tenant", UserID: "user"}
	encoded, err := EncodeMessage(want)
	if err != nil {
		t.Fatalf("EncodeMessage() error = %v", err)
	}
	decoded, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("DecodeMessage() error = %v", err)
	}
	got, ok := decoded.(UserEventV1)
	if !ok {
		t.Fatalf("DecodeMessage() type = %T, want UserEventV1", decoded)
	}
	if got.Type != want.Type || got.TenantID != want.TenantID || got.UserID != want.UserID {
		t.Fatalf("DecodeMessage() = %+v, want %+v", got, want)
	}
}

func TestDecodeMessageRejectsUnsupportedSchemaVersion(t *testing.T) {
	t.Parallel()

	_, err := DecodeMessage([]byte(`{"type":"user_event","schema_version":2,"payload":{}}`))
	if err == nil {
		t.Fatal("DecodeMessage() error = nil")
	}
}
