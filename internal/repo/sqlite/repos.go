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
	return newTxRepos(f.backend, tx)
}

func NewRepos(backend *Backend, tx *sql.Tx) uow.Repos {
	return newTxRepos(backend, tx)
}

func (f Factory) TTL(tx *sql.Tx) TTLRepo {
	return ttlRepo{sqlTxRepo: sqlTxRepo{backend: f.backend, tx: tx}}
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

func (r TxRepo) Tables() uow.TableRepo   { return tableRepo{sqlTxRepo: r.sqlTxRepo} }
func (r TxRepo) Items() uow.ItemRepo     { return itemRepo{sqlTxRepo: r.sqlTxRepo} }
func (r TxRepo) Streams() uow.StreamRepo { return streamRepo{sqlTxRepo: r.sqlTxRepo} }
func (r TxRepo) PITR() uow.PITRRepo      { return pitrRepo{sqlTxRepo: r.sqlTxRepo} }
func (r TxRepo) Backups() uow.BackupRepo { return backupRepo{sqlTxRepo: r.sqlTxRepo} }
func (r TxRepo) ResourcePolicies() uow.ResourcePolicyRepo {
	return resourcePolicyRepo{sqlTxRepo: r.sqlTxRepo}
}

func (r TxRepo) PITRRepo() pitrRepo   { return pitrRepo{sqlTxRepo: r.sqlTxRepo} }
func (r TxRepo) ItemRepo() itemRepo   { return itemRepo{sqlTxRepo: r.sqlTxRepo} }
func (r TxRepo) TableRepo() tableRepo { return tableRepo{sqlTxRepo: r.sqlTxRepo} }
func (r TxRepo) TTLRepo() ttlRepo     { return ttlRepo{sqlTxRepo: r.sqlTxRepo} }

type txRepos struct {
	tables   *tableRepo
	items    *itemRepo
	streams  *streamRepo
	pitr     *pitrRepo
	backups  *backupRepo
	policies *resourcePolicyRepo
}

func newTxRepos(backend *Backend, tx *sql.Tx) txRepos {
	shared := sqlTxRepo{backend: backend, tx: tx}
	return txRepos{
		tables:   &tableRepo{sqlTxRepo: shared},
		items:    &itemRepo{sqlTxRepo: shared},
		streams:  &streamRepo{sqlTxRepo: shared},
		pitr:     &pitrRepo{sqlTxRepo: shared},
		backups:  &backupRepo{sqlTxRepo: shared},
		policies: &resourcePolicyRepo{sqlTxRepo: shared},
	}
}

func (r txRepos) Tables() uow.TableRepo                    { return r.tables }
func (r txRepos) Items() uow.ItemRepo                      { return r.items }
func (r txRepos) Streams() uow.StreamRepo                  { return r.streams }
func (r txRepos) PITR() uow.PITRRepo                       { return r.pitr }
func (r txRepos) Backups() uow.BackupRepo                  { return r.backups }
func (r txRepos) ResourcePolicies() uow.ResourcePolicyRepo { return r.policies }

type tableRepo struct {
	sqlTxRepo
}

type itemRepo struct {
	sqlTxRepo
}

type streamRepo struct {
	sqlTxRepo
}

type pitrRepo struct {
	sqlTxRepo
}

type backupRepo struct {
	sqlTxRepo
}

type resourcePolicyRepo struct {
	sqlTxRepo
}

type ttlRepo struct {
	sqlTxRepo
}
