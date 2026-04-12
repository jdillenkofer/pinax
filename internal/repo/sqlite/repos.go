package sqlite

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pinax/internal/app/uow"
)

type TTLRepo interface {
	GetExpiredItems(ctx context.Context, tableKey string, ttlAttr string, before int64, limit int) ([]struct {
		PK string
		SK string
	}, error)
	DeleteExpiredItem(ctx context.Context, tableKey, pk, sk string) error
}

type Factory struct {
	backend *Backend
}

func NewFactory(backend *Backend) Factory {
	return Factory{backend: backend}
}

func (f Factory) Build(tx *sql.Tx) uow.Repos {
	return txRepos{repo: sqlTxRepo{backend: f.backend, tx: tx}}
}

func NewRepos(backend *Backend, tx *sql.Tx) uow.Repos {
	return txRepos{repo: sqlTxRepo{backend: backend, tx: tx}}
}

func (f Factory) TTL(tx *sql.Tx) TTLRepo {
	return sqlTxRepo{backend: f.backend, tx: tx}
}

type txRepos struct {
	repo sqlTxRepo
}

type sqlTxRepo struct {
	backend *Backend
	tx      *sql.Tx
}

type TxRepo struct {
	sqlTxRepo
}

func NewTxRepo(backend *Backend, tx *sql.Tx) TxRepo {
	return TxRepo{sqlTxRepo: sqlTxRepo{backend: backend, tx: tx}}
}

func (r txRepos) Tables() uow.TableRepo                    { return r.repo }
func (r txRepos) Items() uow.ItemRepo                      { return r.repo }
func (r txRepos) Streams() uow.StreamRepo                  { return r.repo }
func (r txRepos) PITR() uow.PITRRepo                       { return r.repo }
func (r txRepos) Backups() uow.BackupRepo                  { return r.repo }
func (r txRepos) ResourcePolicies() uow.ResourcePolicyRepo { return r.repo }
