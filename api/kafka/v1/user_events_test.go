package v1

import (
	"bytes"
	"testing"
)

func TestUserEventV1Validate(t *testing.T) {
	testCases := []struct {
		name  string
		event UserEventV1
		valid bool
	}{
		{name: "created", event: UserEventV1{Type: UserEventTypeCreated, TenantID: "tenant", UserID: "user"}, valid: true},
		{name: "updated", event: UserEventV1{Type: UserEventTypeUpdated, TenantID: "tenant", UserID: "user"}, valid: true},
		{name: "deleted", event: UserEventV1{Type: UserEventTypeDeleted, TenantID: "tenant", UserID: "user"}, valid: true},
		{name: "unsupported type", event: UserEventV1{Type: "other", TenantID: "tenant", UserID: "user"}},
		{name: "empty tenant", event: UserEventV1{Type: UserEventTypeCreated, UserID: "user"}},
		{name: "empty user", event: UserEventV1{Type: UserEventTypeCreated, TenantID: "tenant"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.event.Validate()
			if testCase.valid && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if !testCase.valid && err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestPartitionKeyEscapesOpaqueIdentifiers(t *testing.T) {
	key := PartitionKey("tenant:one", "user\"two")
	if !bytes.Equal(key, []byte(`["tenant:one","user\"two"]`)) {
		t.Fatalf("PartitionKey() = %s", key)
	}
}
