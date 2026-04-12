package sqlite

import (
	"database/sql"

	"github.com/jdillenkofer/pinax/internal/app/uow"
)

func (f Factory) TTL(tx *sql.Tx) uow.TTLRepo {
	return ttlRepo{sqlTxRepo: sqlTxRepo{backend: f.backend, tx: tx}}
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

type sqlTxRepo struct {
	backend *Backend
	tx      *sql.Tx
}

type txRepos struct {
	tables   *tableRepo
	items    *itemRepo
	streams  *streamRepo
	pitr     *pitrRepo
	backups  *backupRepo
	policies *resourcePolicyRepo
	ttl      *ttlRepo
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
		ttl:      &ttlRepo{sqlTxRepo: shared},
	}
}

func (r txRepos) Tables() uow.TableRepo                    { return r.tables }
func (r txRepos) Items() uow.ItemRepo                      { return r.items }
func (r txRepos) Streams() uow.StreamRepo                  { return r.streams }
func (r txRepos) PITR() uow.PITRRepo                       { return r.pitr }
func (r txRepos) Backups() uow.BackupRepo                  { return r.backups }
func (r txRepos) ResourcePolicies() uow.ResourcePolicyRepo { return r.policies }
func (r txRepos) TTL() uow.TTLRepo                         { return r.ttl }

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
