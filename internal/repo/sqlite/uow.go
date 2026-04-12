package sqlite

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pinax/internal/app/uow"
)

type unitOfWork struct {
	db      *sql.DB
	factory Factory
}

type txContextKey struct{}

func NewUnitOfWork(db *sql.DB, factory Factory) uow.UnitOfWork {
	return &unitOfWork{db: db, factory: factory}
}

func (u *unitOfWork) Do(ctx context.Context, fn func(ctx context.Context, repos uow.Repos) error) error {
	if ambient, ok := ctx.Value(txContextKey{}).(*sql.Tx); ok && ambient != nil {
		return fn(ctx, u.factory.Build(ambient))
	}

	tx, err := u.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	txCtx := context.WithValue(ctx, txContextKey{}, tx)
	if err := fn(txCtx, u.factory.Build(tx)); err != nil {
		return err
	}
	return tx.Commit()
}
