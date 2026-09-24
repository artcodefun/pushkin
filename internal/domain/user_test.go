package domain

import (
	"testing"
	"uuid"
)

func TestUserCopiesAttributesAndDeletes(t *testing.T) {
	t.Parallel()
	attributes := map[string]string{"name": "Ada"}
	user, err := NewUser(NewUserParams{TenantID: uuid.NewV7(), UserID: "user-1", Attributes: attributes})
	if err != nil {
		t.Fatalf("new user: %v", err)
	}
	attributes["name"] = "Grace"
	copy := user.Attributes()
	copy["name"] = "Lin"
	if user.Attributes()["name"] != "Ada" {
		t.Fatal("attributes must be copied")
	}
	user.Delete()
	if user.Status() != UserStatusDeleted {
		t.Fatalf("unexpected status: %s", user.Status())
	}
}
