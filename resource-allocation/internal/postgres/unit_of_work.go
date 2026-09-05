package postgres

import (
	"context"
	"fmt"

	"cs-ra/internal/resource"

	"github.com/jackc/pgx/v5/pgxpool"
)

type unitOfWork struct {
	pool *pgxpool.Pool
}

func NewUnitOfWork(
	pool *pgxpool.Pool,
) resource.UnitOfWork {
	return &unitOfWork{
		pool: pool,
	}
}

func (u *unitOfWork) WithinTransaction(
	ctx context.Context,
	fn func(repository resource.Repository) error,
) error {
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"begin transaction: %w",
			err,
		)
	}

	committed := false

	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	repository := newEntitlementRepository(tx)

	if err := fn(repository); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit transaction: %w",
			err,
		)
	}

	committed = true

	return nil
}
