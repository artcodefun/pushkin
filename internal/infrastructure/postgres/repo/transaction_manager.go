package repo

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/superman/pushkin/internal/application/ports"
)

var _ ports.TransactionManager = (*TransactionManager)(nil)

type TransactionManager struct {
	pool *pgxpool.Pool
}

func NewTransactionManager(pool *pgxpool.Pool) *TransactionManager {
	return &TransactionManager{pool: pool}
}

func (m *TransactionManager) WithTx(ctx context.Context, fn func(tx ports.Transaction) error) (err error) {
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = tx.Rollback(ctx)
			panic(recovered)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
