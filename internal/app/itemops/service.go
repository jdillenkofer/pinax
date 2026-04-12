package itemops

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/model"
)

type ValidationError struct {
	Message string
}

func (e ValidationError) Error() string {
	return e.Message
}

func invalid(message string) error {
	return ValidationError{Message: message}
}

type Service struct {
	unitOfWork     uow.UnitOfWork
	getActiveTable func(context.Context, uow.TableRepo, string) (model.Table, error)
}

func NewService(unitOfWork uow.UnitOfWork, getActiveTable func(context.Context, uow.TableRepo, string) (model.Table, error)) *Service {
	return &Service{unitOfWork: unitOfWork, getActiveTable: getActiveTable}
}

type BatchGetTableRequest struct {
	Keys           []map[string]any
	ConsistentRead bool
	Project        func(map[string]any) (map[string]any, error)
}

type BatchGetInput struct {
	RequestItems map[string]BatchGetTableRequest
	ProcessLimit int
	ReserveRead  func(model.Table, float64) bool
}

type BatchGetResult struct {
	Responses   map[string][]map[string]any
	Unprocessed map[string][]map[string]any
	ReadByTable map[string]float64
}

func (s *Service) BatchGet(ctx context.Context, input BatchGetInput) (BatchGetResult, error) {
	processed := 0
	result := BatchGetResult{
		Responses:   map[string][]map[string]any{},
		Unprocessed: map[string][]map[string]any{},
		ReadByTable: map[string]float64{},
	}
	err := s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		for tableName, itemReq := range input.RequestItems {
			t, err := s.getActiveTable(txCtx, repos.Tables(), tableName)
			if err != nil {
				return err
			}
			items := make([]map[string]any, 0, len(itemReq.Keys))
			unprocessedKeys := make([]map[string]any, 0)
			seenKeys := map[string]struct{}{}
			for _, key := range itemReq.Keys {
				pk, sk, err := model.ExtractKey(t, key)
				if err != nil {
					return invalid(err.Error())
				}
				target := tableName + "|" + pk + "|" + sk
				if _, exists := seenKeys[target]; exists {
					return invalid("Provided list of item keys contains duplicates")
				}
				seenKeys[target] = struct{}{}
				if input.ProcessLimit > 0 && processed >= input.ProcessLimit {
					unprocessedKeys = append(unprocessedKeys, key)
					continue
				}
				processed++

				item, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				if err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						units := model.CalculateReadCapacityUnits(1, itemReq.ConsistentRead)
						if !input.ReserveRead(t, units) {
							unprocessedKeys = append(unprocessedKeys, key)
							continue
						}
						result.ReadByTable[tableName] += units
						continue
					}
					return err
				}
				units := model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), itemReq.ConsistentRead)
				if !input.ReserveRead(t, units) {
					unprocessedKeys = append(unprocessedKeys, key)
					continue
				}
				projected, err := itemReq.Project(item)
				if err != nil {
					return invalid(err.Error())
				}
				items = append(items, projected)
				result.ReadByTable[tableName] += units
			}
			result.Responses[tableName] = items
			if len(unprocessedKeys) > 0 {
				result.Unprocessed[tableName] = unprocessedKeys
			}
		}
		return nil
	})
	return result, err
}

type BatchWriteOperation struct {
	PutItem    map[string]any
	DeleteKey  map[string]any
	RawRequest any
}

type BatchWriteInput struct {
	RequestItems       map[string][]BatchWriteOperation
	ProcessLimit       int
	IncludeItemMetrics bool
	ReserveWrite       func(model.Table, float64) bool
	EmitMutation       func(context.Context, uow.Repos, model.Table, string, map[string]any, map[string]any, map[string]any, int64) error
}

type BatchWriteResult struct {
	Unprocessed           map[string][]any
	WriteByTable          map[string]float64
	ItemCollectionMetrics map[string][]map[string]any
}

func (s *Service) BatchWrite(ctx context.Context, input BatchWriteInput) (BatchWriteResult, error) {
	processed := 0
	result := BatchWriteResult{
		Unprocessed:           map[string][]any{},
		WriteByTable:          map[string]float64{},
		ItemCollectionMetrics: map[string][]map[string]any{},
	}
	tableNames := make([]string, 0, len(input.RequestItems))
	for tableName := range input.RequestItems {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)

	err := s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		for _, tableName := range tableNames {
			ops := input.RequestItems[tableName]
			t, err := s.getActiveTable(txCtx, repos.Tables(), tableName)
			if err != nil {
				return err
			}
			seenKeys := map[string]struct{}{}
			seenItemCollections := map[string]struct{}{}
			for _, op := range ops {
				if len(op.PutItem) == 0 && len(op.DeleteKey) == 0 {
					return invalid("write request must contain either PutRequest or DeleteRequest")
				}
				if len(op.PutItem) > 0 && len(op.DeleteKey) > 0 {
					return invalid("write request cannot contain both PutRequest and DeleteRequest")
				}
				if input.ProcessLimit > 0 && processed >= input.ProcessLimit {
					result.Unprocessed[tableName] = append(result.Unprocessed[tableName], op.RawRequest)
					continue
				}

				if len(op.PutItem) > 0 {
					if model.ItemTooLarge(op.PutItem) {
						return invalid("Item size has exceeded the maximum allowed size")
					}
					pk, sk, err := model.ExtractItemKeys(t, op.PutItem)
					if err != nil {
						return invalid(err.Error())
					}
					if err := model.ValidateSecondaryIndexKeyTypes(t, op.PutItem); err != nil {
						return invalid(err.Error())
					}
					k := tableName + "|" + pk + "|" + sk
					if _, exists := seenKeys[k]; exists {
						return invalid("Provided list of item keys contains duplicates")
					}
					seenKeys[k] = struct{}{}

					current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
					existed := true
					if err != nil && !errors.Is(err, sql.ErrNoRows) {
						return err
					}
					if errors.Is(err, sql.ErrNoRows) {
						existed = false
						current = map[string]any{}
					}

					writeUnits := model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(op.PutItem))
					if !input.ReserveWrite(t, writeUnits) {
						result.Unprocessed[tableName] = append(result.Unprocessed[tableName], op.RawRequest)
						continue
					}
					if err := repos.Items().PutItem(txCtx, t.Name, pk, sk, op.PutItem); err != nil {
						return err
					}
					eventName := "INSERT"
					streamOld := current
					if existed {
						eventName = "MODIFY"
					} else {
						streamOld = nil
					}
					key := map[string]any{t.HashKey: op.PutItem[t.HashKey]}
					if t.RangeKey != "" {
						key[t.RangeKey] = op.PutItem[t.RangeKey]
					}
					if err := input.EmitMutation(txCtx, repos, t, eventName, key, streamOld, op.PutItem, time.Now().UnixMilli()); err != nil {
						return err
					}
					result.WriteByTable[tableName] += writeUnits
					if input.IncludeItemMetrics && len(t.LSIs) > 0 {
						hashValue := op.PutItem[t.HashKey]
						if keyToken, err := model.SerializeKeyValue(hashValue); err == nil {
							if _, exists := seenItemCollections[keyToken]; !exists {
								seenItemCollections[keyToken] = struct{}{}
								result.ItemCollectionMetrics[tableName] = append(result.ItemCollectionMetrics[tableName], map[string]any{
									"ItemCollectionKey":   map[string]any{t.HashKey: hashValue},
									"SizeEstimateRangeGB": []float64{0, 1},
								})
							}
						}
					}
					processed++
				}

				if len(op.DeleteKey) > 0 {
					pk, sk, err := model.ExtractKey(t, op.DeleteKey)
					if err != nil {
						return invalid(err.Error())
					}
					k := tableName + "|" + pk + "|" + sk
					if _, exists := seenKeys[k]; exists {
						return invalid("Provided list of item keys contains duplicates")
					}
					seenKeys[k] = struct{}{}

					current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
					existed := true
					if err != nil && !errors.Is(err, sql.ErrNoRows) {
						return err
					}
					if errors.Is(err, sql.ErrNoRows) {
						existed = false
						current = op.DeleteKey
					}
					writeUnits := model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(current))
					if !input.ReserveWrite(t, writeUnits) {
						result.Unprocessed[tableName] = append(result.Unprocessed[tableName], op.RawRequest)
						continue
					}
					if err := repos.Items().DeleteItem(txCtx, t.Name, pk, sk); err != nil {
						return err
					}
					if existed && len(current) > 0 {
						if err := input.EmitMutation(txCtx, repos, t, "REMOVE", op.DeleteKey, current, nil, time.Now().UnixMilli()); err != nil {
							return err
						}
					}
					result.WriteByTable[tableName] += writeUnits
					if input.IncludeItemMetrics && len(t.LSIs) > 0 {
						hashValue := op.DeleteKey[t.HashKey]
						if keyToken, err := model.SerializeKeyValue(hashValue); err == nil {
							if _, exists := seenItemCollections[keyToken]; !exists {
								seenItemCollections[keyToken] = struct{}{}
								result.ItemCollectionMetrics[tableName] = append(result.ItemCollectionMetrics[tableName], map[string]any{
									"ItemCollectionKey":   map[string]any{t.HashKey: hashValue},
									"SizeEstimateRangeGB": []float64{0, 1},
								})
							}
						}
					}
					processed++
				}
			}
		}
		return nil
	})
	return result, err
}
