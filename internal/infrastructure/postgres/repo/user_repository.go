package repo

import (
	"context"
	"encoding/json"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
)

var _ ports.UserRepository = (*UserRepository)(nil)

type UserRepository struct {
	queries *gen.Queries
}

func NewUserRepository(queries *gen.Queries) *UserRepository {
	return &UserRepository{queries: queries}
}

func (r *UserRepository) Save(ctx context.Context, user *domain.User) error {
	attributes, err := json.Marshal(user.Attributes())
	if err != nil {
		return err
	}
	return r.queries.SaveUser(ctx, gen.SaveUserParams{
		TenantID:   user.TenantID(),
		UserID:     string(user.ID()),
		Attributes: attributes,
		Status:     string(user.Status()),
	})
}
