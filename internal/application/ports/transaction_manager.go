package ports

import "context"

// Transaction is an opaque handle used only to bind repositories to one
// database transaction. Commit and rollback remain the TransactionManager's
// responsibility.
type Transaction interface{}

type TransactionManager interface {
	WithTx(ctx context.Context, fn func(tx Transaction) error) error
}
