package query

import (
	"context"
	"fmt"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/model"
)

type ErrUnknownIndex struct {
	IndexName string
}

func (e ErrUnknownIndex) Error() string {
	return fmt.Sprintf("unknown index %s", e.IndexName)
}

type ErrConsistentReadOnGSI struct{}

func (ErrConsistentReadOnGSI) Error() string {
	return "consistent read on gsi"
}

type ErrIndexNotActive struct {
	IndexName string
}

func (e ErrIndexNotActive) Error() string {
	return fmt.Sprintf("index %s is not active", e.IndexName)
}

type Service struct {
	unitOfWork     uow.UnitOfWork
	getActiveTable func(context.Context, uow.TableRepo, string) (model.Table, error)
}

func NewService(unitOfWork uow.UnitOfWork, getActiveTable func(context.Context, uow.TableRepo, string) (model.Table, error)) *Service {
	return &Service{unitOfWork: unitOfWork, getActiveTable: getActiveTable}
}

type QueryTarget struct {
	Table           model.Table
	TargetHashKey   string
	TargetHashType  string
	TargetRangeKey  string
	TargetRangeType string
	QueryGSI        *model.GlobalSecondaryIndex
	QueryLSI        *model.LocalSecondaryIndex
}

type ResolveQueryTargetInput struct {
	TableName      string
	IndexName      string
	ConsistentRead bool
}

func (s *Service) ResolveQueryTarget(ctx context.Context, input ResolveQueryTargetInput) (QueryTarget, error) {
	var target QueryTarget
	err := s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		t, err := s.getActiveTable(txCtx, repos.Tables(), input.TableName)
		if err != nil {
			return err
		}
		target = QueryTarget{
			Table:           t,
			TargetHashKey:   t.HashKey,
			TargetHashType:  t.HashType,
			TargetRangeKey:  t.RangeKey,
			TargetRangeType: t.RangeType,
		}
		return nil
	})
	if err != nil {
		return QueryTarget{}, err
	}

	if input.IndexName == "" {
		return target, nil
	}
	if gsi, ok := target.Table.GetGSI(input.IndexName); ok {
		if gsi.Status != "" && gsi.Status != model.IndexStatusActive {
			return QueryTarget{}, ErrIndexNotActive{IndexName: input.IndexName}
		}
		if input.ConsistentRead {
			return QueryTarget{}, ErrConsistentReadOnGSI{}
		}
		target.QueryGSI = &gsi
		target.TargetHashKey = gsi.HashKey
		target.TargetRangeKey = gsi.RangeKey
		target.TargetHashType = gsi.HashType
		target.TargetRangeType = gsi.RangeType
		return target, nil
	}
	if lsi, ok := target.Table.GetLSI(input.IndexName); ok {
		target.QueryLSI = &lsi
		target.TargetHashKey = target.Table.HashKey
		target.TargetRangeKey = lsi.RangeKey
		target.TargetHashType = target.Table.HashType
		target.TargetRangeType = lsi.RangeType
		return target, nil
	}
	return QueryTarget{}, ErrUnknownIndex{IndexName: input.IndexName}
}

type QueryItemsInput struct {
	Target              QueryTarget
	IndexName           string
	PK                  string
	ExclusiveStartKey   map[string]any
	ScanForward         bool
	HasSortKeyCondition bool
	Orderer             QueryOrderer
}

type QueryOrderer interface {
	OrderItemsForGSI([]map[string]any, model.Table, model.GlobalSecondaryIndex, map[string]any, bool) ([]map[string]any, error)
	OrderItemsForLSI([]map[string]any, model.Table, model.LocalSecondaryIndex, map[string]any, bool) ([]map[string]any, error)
	OrderItemsForTable([]map[string]any, model.Table, map[string]any, bool) ([]map[string]any, error)
}

func (s *Service) QueryItems(ctx context.Context, input QueryItemsInput) ([]map[string]any, error) {
	var items []map[string]any
	err := s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		var err error
		t := input.Target.Table
		if input.Target.QueryGSI != nil {
			items, err = repos.Items().QueryByGSI(txCtx, t.Name, input.IndexName, input.PK, "", true, 0)
			if err != nil {
				return err
			}
			items, err = input.Orderer.OrderItemsForGSI(items, t, *input.Target.QueryGSI, input.ExclusiveStartKey, input.ScanForward)
			return err
		}
		if input.Target.QueryLSI != nil {
			items, err = repos.Items().QueryByGSI(txCtx, t.Name, input.IndexName, input.PK, "", true, 0)
			if err != nil {
				return err
			}
			items, err = input.Orderer.OrderItemsForLSI(items, t, *input.Target.QueryLSI, input.ExclusiveStartKey, input.ScanForward)
			return err
		}
		if !input.HasSortKeyCondition {
			if input.Target.TargetRangeKey == "" {
				items, err = repos.Items().QueryByPKSK(txCtx, t.Name, input.PK, model.NoSortKey)
				return err
			}
			items, err = repos.Items().QueryByPK(txCtx, t.Name, input.PK, "", true, 0)
			if err != nil {
				return err
			}
			items, err = input.Orderer.OrderItemsForTable(items, t, input.ExclusiveStartKey, input.ScanForward)
			return err
		}

		items, err = repos.Items().QueryByPK(txCtx, t.Name, input.PK, "", true, 0)
		if err != nil {
			return err
		}
		items, err = input.Orderer.OrderItemsForTable(items, t, input.ExclusiveStartKey, input.ScanForward)
		return err
	})
	return items, err
}

type ScanItemsInput struct {
	TableName       string
	ResolveStartKey func(model.Table) (string, string, error)
}

func (s *Service) ScanItems(ctx context.Context, input ScanItemsInput) (model.Table, []map[string]any, error) {
	var (
		table model.Table
		items []map[string]any
	)
	err := s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		var err error
		table, err = s.getActiveTable(txCtx, repos.Tables(), input.TableName)
		if err != nil {
			return err
		}
		startPK, startSK := "", ""
		if input.ResolveStartKey != nil {
			startPK, startSK, err = input.ResolveStartKey(table)
			if err != nil {
				return err
			}
		}
		items, err = repos.Items().Scan(txCtx, table.Name, startPK, startSK, 0)
		return err
	})
	return table, items, err
}

type QueryProcessInput struct {
	Table          model.Table
	Items          []map[string]any
	Limit          int
	SelectMode     string
	ConsistentRead bool
	IndexName      string
	TargetHashKey  string
	PK             string
	QueryGSI       *model.GlobalSecondaryIndex
	QueryLSI       *model.LocalSecondaryIndex
	Processor      QueryProcessor
}

type QueryProcessor interface {
	ProjectItemForGSI(map[string]any, model.Table, model.GlobalSecondaryIndex) map[string]any
	ProjectItemForLSI(map[string]any, model.Table, model.LocalSecondaryIndex) map[string]any
	SortConditionMatches(map[string]any) (bool, error)
	ApplyFilter(map[string]any) (bool, error)
	ApplyProjection(map[string]any) (map[string]any, error)
	CloneItem(map[string]any) map[string]any
	KeyFromItem(model.Table, map[string]any) map[string]any
	SegmentForPK(string, int) int
}

type QueryProcessResult struct {
	Count       int
	Scanned     int
	Items       []map[string]any
	LastScanned map[string]any
	TotalRead   float64
}

func (s *Service) ProcessQueryItems(input QueryProcessInput) (QueryProcessResult, error) {
	res := QueryProcessResult{Items: make([]map[string]any, 0)}
	for _, item := range input.Items {
		queryItem := item
		if input.QueryGSI != nil {
			queryItem = input.Processor.ProjectItemForGSI(item, input.Table, *input.QueryGSI)
		} else if input.QueryLSI != nil {
			queryItem = input.Processor.ProjectItemForLSI(item, input.Table, *input.QueryLSI)
		}

		if input.IndexName != "" {
			raw, ok := queryItem[input.TargetHashKey]
			if !ok {
				continue
			}
			itemPK, err := model.SerializeKeyValue(raw)
			if err != nil || itemPK != input.PK {
				continue
			}
		}

		if input.Processor != nil {
			ok, err := input.Processor.SortConditionMatches(queryItem)
			if err != nil {
				return QueryProcessResult{}, err
			}
			if !ok {
				continue
			}
		}

		res.Scanned++
		res.LastScanned = input.Processor.KeyFromItem(input.Table, item)
		res.TotalRead += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(queryItem), input.ConsistentRead)

		matches, err := input.Processor.ApplyFilter(queryItem)
		if err != nil {
			return QueryProcessResult{}, err
		}
		if matches {
			if input.SelectMode != "COUNT" {
				emit := queryItem
				switch input.SelectMode {
				case "ALL_ATTRIBUTES":
					emit = item
				case "SPECIFIC_ATTRIBUTES":
					projected, err := input.Processor.ApplyProjection(queryItem)
					if err != nil {
						return QueryProcessResult{}, err
					}
					emit = projected
				}
				res.Items = append(res.Items, input.Processor.CloneItem(emit))
			}
			res.Count++
		}

		if input.Limit > 0 && res.Scanned >= input.Limit {
			break
		}
	}
	return res, nil
}

type ScanProcessInput struct {
	Table          model.Table
	Items          []map[string]any
	Limit          int
	SegmentEnabled bool
	Segment        int
	TotalSegments  int
	ConsistentRead bool
	Processor      QueryProcessor
}

type ScanProcessResult struct {
	Items       []map[string]any
	Scanned     int
	LastScanned map[string]any
	TotalRead   float64
}

func (s *Service) ProcessScanItems(input ScanProcessInput) (ScanProcessResult, error) {
	res := ScanProcessResult{Items: make([]map[string]any, 0)}
	for _, item := range input.Items {
		if input.SegmentEnabled {
			rawPK, ok := item[input.Table.HashKey]
			if !ok {
				continue
			}
			serializedPK, err := model.SerializeKeyValue(rawPK)
			if err != nil {
				continue
			}
			if input.Processor.SegmentForPK(serializedPK, input.TotalSegments) != input.Segment {
				continue
			}
		}

		res.Scanned++
		res.LastScanned = input.Processor.KeyFromItem(input.Table, item)
		res.TotalRead += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), input.ConsistentRead)

		matches, err := input.Processor.ApplyFilter(item)
		if err != nil {
			return ScanProcessResult{}, err
		}
		if !matches {
			if input.Limit > 0 && res.Scanned >= input.Limit {
				break
			}
			continue
		}
		projected, err := input.Processor.ApplyProjection(item)
		if err != nil {
			return ScanProcessResult{}, err
		}
		res.Items = append(res.Items, projected)
		if input.Limit > 0 && res.Scanned >= input.Limit {
			break
		}
	}
	return res, nil
}
