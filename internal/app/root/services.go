package root

import (
	"context"

	backupapp "github.com/jdillenkofer/pinax/internal/app/backup"
	itemopsapp "github.com/jdillenkofer/pinax/internal/app/itemops"
	pitrapp "github.com/jdillenkofer/pinax/internal/app/pitr"
	queryapp "github.com/jdillenkofer/pinax/internal/app/query"
	resourcepolicyapp "github.com/jdillenkofer/pinax/internal/app/resourcepolicy"
	tableapp "github.com/jdillenkofer/pinax/internal/app/table"
	transactionapp "github.com/jdillenkofer/pinax/internal/app/transaction"
	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/model"
)

type ActiveTableResolver func(context.Context, uow.TableRepo, string) (model.Table, error)

type Services struct {
	TableLifecycle        *tableapp.LifecycleService
	TableService          *tableapp.Service
	QueryService          *queryapp.Service
	ItemOpsService        *itemopsapp.Service
	TransactionService    *transactionapp.Service
	BackupService         *backupapp.Service
	PITRService           *pitrapp.Service
	ResourcePolicyService *resourcepolicyapp.Service
}

func NewServices(unitOfWork uow.UnitOfWork, activeTableResolver ActiveTableResolver) Services {
	tableLifecycle := tableapp.NewLifecycleService()
	return Services{
		TableLifecycle:        tableLifecycle,
		TableService:          tableapp.NewService(unitOfWork, tableLifecycle),
		QueryService:          queryapp.NewService(unitOfWork, activeTableResolver),
		ItemOpsService:        itemopsapp.NewService(unitOfWork, activeTableResolver),
		TransactionService:    transactionapp.NewService(unitOfWork, activeTableResolver),
		BackupService:         backupapp.NewService(unitOfWork, tableLifecycle),
		PITRService:           pitrapp.NewService(unitOfWork, tableLifecycle),
		ResourcePolicyService: resourcepolicyapp.NewService(unitOfWork, tableLifecycle),
	}
}
