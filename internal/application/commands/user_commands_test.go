package commands

import (
	"context"
	"testing"
	"uuid"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/domain"
)

func TestUserCommandsApplyExternalEvents(t *testing.T) {
	t.Parallel()
	repository := &fakeUserRepository{}
	commands := NewUserCommands(UserCommandsParams{UserRepository: repository})
	err := commands.UpsertUser(context.Background(), application.UpsertUserCommand{TenantID: uuid.NewV7(), UserID: "user-1", Attributes: map[string]string{"name": "Ada"}})
	if err != nil || repository.user.Status() != domain.UserStatusActive {
		t.Fatalf("upsert: err=%v user=%+v", err, repository.user)
	}
	err = commands.DeleteUser(context.Background(), application.DeleteUserCommand{TenantID: repository.user.TenantID(), UserID: repository.user.ID()})
	if err != nil || repository.user.Status() != domain.UserStatusDeleted {
		t.Fatalf("delete: err=%v user=%+v", err, repository.user)
	}
}

type fakeUserRepository struct {
	user *domain.User
}

func (r *fakeUserRepository) Save(_ context.Context, user *domain.User) error {
	r.user = user
	return nil
}
