package transaction

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/apperr"
	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/model"
)

func invalid(message string) error { return apperr.Validation(message) }

type Service struct {
	unitOfWork     uow.UnitOfWork
	getActiveTable func(context.Context, uow.TableRepo, string) (model.Table, error)
}

func NewService(unitOfWork uow.UnitOfWork, getActiveTable func(context.Context, uow.TableRepo, string) (model.Table, error)) *Service {
	return &Service{unitOfWork: unitOfWork, getActiveTable: getActiveTable}
}

type TransactGetItem struct {
	TableName            string
	Key                  map[string]any
	ProjectionExpression string
	ExpressionNames      map[string]string
}

type TransactGetInput struct {
	Items   []TransactGetItem
	Adapter TransactGetAdapter
}

type TransactGetAdapter interface {
	ApplyProjection(item map[string]any, projection string, names map[string]string) (map[string]any, error)
	EnsureRead(model.Table, float64) error
}

type TransactGetResult struct {
	Responses   []map[string]any
	ReadByTable map[string]float64
}

func (s *Service) TransactGet(ctx context.Context, input TransactGetInput) (TransactGetResult, error) {
	result := TransactGetResult{Responses: make([]map[string]any, 0, len(input.Items)), ReadByTable: map[string]float64{}}
	err := s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		for _, get := range input.Items {
			t, err := s.getActiveTable(txCtx, repos.Tables(), get.TableName)
			if err != nil {
				return err
			}
			pk, sk, err := model.ExtractKey(t, get.Key)
			if err != nil {
				return invalid(err.Error())
			}
			item, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					if _, err := input.Adapter.ApplyProjection(map[string]any{}, get.ProjectionExpression, get.ExpressionNames); err != nil {
						return invalid(err.Error())
					}
					result.ReadByTable[get.TableName] += model.CalculateReadCapacityUnits(1, true)
					result.Responses = append(result.Responses, map[string]any{})
					continue
				}
				return err
			}
			projected, err := input.Adapter.ApplyProjection(item, get.ProjectionExpression, get.ExpressionNames)
			if err != nil {
				return invalid(err.Error())
			}
			result.ReadByTable[get.TableName] += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), true)
			result.Responses = append(result.Responses, map[string]any{"Item": projected})
		}
		for tableName, units := range result.ReadByTable {
			t, err := s.getActiveTable(txCtx, repos.Tables(), tableName)
			if err != nil {
				return err
			}
			if err := input.Adapter.EnsureRead(t, units); err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}

type PutAction struct {
	TableName    string
	Item         map[string]any
	Condition    string
	ReturnOnFail string
	ExprNames    map[string]string
	ExprValues   map[string]any
}

type DeleteAction struct {
	TableName    string
	Key          map[string]any
	Condition    string
	ReturnOnFail string
	ExprNames    map[string]string
	ExprValues   map[string]any
}

type UpdateAction struct {
	TableName        string
	Key              map[string]any
	UpdateExpression string
	Condition        string
	ReturnOnFail     string
	ExprNames        map[string]string
	ExprValues       map[string]any
}

type ConditionCheckAction struct {
	TableName    string
	Key          map[string]any
	Condition    string
	ReturnOnFail string
	ExprNames    map[string]string
	ExprValues   map[string]any
}

type TransactWriteItem struct {
	Put            *PutAction
	Delete         *DeleteAction
	Update         *UpdateAction
	ConditionCheck *ConditionCheckAction
}

type TransactWriteInput struct {
	ClientRequestToken   string
	Items                []TransactWriteItem
	NowMillis            int64
	IdempotencyTTLMillis int64

	Adapter TransactWriteAdapter
}

type TransactWriteAdapter interface {
	ValidateReturnOnFail(string) error
	EvaluateCondition(expression string, item map[string]any, names map[string]string, values map[string]any) (bool, error)
	ApplyUpdate(current map[string]any, updateExpression string, names map[string]string, values map[string]any) (map[string]any, error)
	EmitMutation(context.Context, uow.Repos, model.Table, string, map[string]any, map[string]any, map[string]any, int64) error
	EnsureWrite(model.Table, float64) error
	OnConditionEvalError(total int, failedIndex int, message string) error
	OnConditionFailed(total int, failedIndex int, returnValues string, current map[string]any, existed bool) error
	BuildResponse(writeByTable map[string]float64) map[string]any
	RequestHash() (string, error)
	IdempotentMismatchErr() error
}

type TransactWriteResult struct {
	Response map[string]any
}

func (s *Service) TransactWrite(ctx context.Context, input TransactWriteInput) (TransactWriteResult, error) {
	result := TransactWriteResult{}
	err := s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		now := input.NowMillis
		if now <= 0 {
			now = time.Now().UnixMilli()
		}
		if input.ClientRequestToken != "" {
			if err := repos.Items().DeleteExpiredTransactWriteIdempotency(txCtx, now); err != nil {
				return err
			}
			rec, err := repos.Items().GetTransactWriteIdempotency(txCtx, input.ClientRequestToken, now)
			if err == nil {
				hash, err := input.Adapter.RequestHash()
				if err != nil {
					return err
				}
				if rec.RequestHash != hash {
					return input.Adapter.IdempotentMismatchErr()
				}
				result.Response = rec.Response
				return nil
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}

		seenTargets := map[string]struct{}{}
		writeByTable := map[string]float64{}

		for i, item := range input.Items {
			actions := 0
			if item.Put != nil {
				actions++
			}
			if item.Delete != nil {
				actions++
			}
			if item.Update != nil {
				actions++
			}
			if item.ConditionCheck != nil {
				actions++
			}
			if actions != 1 {
				return invalid("TransactItems can only contain one of Check, Put, Update or Delete")
			}

			if item.Put != nil {
				put := item.Put
				if err := input.Adapter.ValidateReturnOnFail(put.ReturnOnFail); err != nil {
					return invalid(err.Error())
				}
				t, err := s.getActiveTable(txCtx, repos.Tables(), put.TableName)
				if err != nil {
					return err
				}
				pk, sk, err := model.ExtractItemKeys(t, put.Item)
				if err != nil {
					return invalid(err.Error())
				}
				target := t.Name + "|" + pk + "|" + sk
				if _, exists := seenTargets[target]; exists {
					return invalid("Transaction request cannot include multiple operations on one item")
				}
				seenTargets[target] = struct{}{}

				current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				existed := true
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if errors.Is(err, sql.ErrNoRows) {
					existed = false
					current = map[string]any{}
				}

				ok, err := input.Adapter.EvaluateCondition(put.Condition, current, put.ExprNames, put.ExprValues)
				if err != nil {
					return input.Adapter.OnConditionEvalError(len(input.Items), i, err.Error())
				}
				if !ok {
					return input.Adapter.OnConditionFailed(len(input.Items), i, put.ReturnOnFail, current, existed)
				}
				if model.ItemTooLarge(put.Item) {
					return invalid("Item size has exceeded the maximum allowed size")
				}
				if err := model.ValidateSecondaryIndexKeyTypes(t, put.Item); err != nil {
					return input.Adapter.OnConditionEvalError(len(input.Items), i, err.Error())
				}
				if err := repos.Items().PutItem(txCtx, t.Name, pk, sk, put.Item); err != nil {
					return err
				}

				eventName := "INSERT"
				oldImage := current
				if existed {
					eventName = "MODIFY"
				} else {
					oldImage = nil
				}
				key := map[string]any{t.HashKey: put.Item[t.HashKey]}
				if t.RangeKey != "" {
					key[t.RangeKey] = put.Item[t.RangeKey]
				}
				if err := input.Adapter.EmitMutation(txCtx, repos, t, eventName, key, oldImage, put.Item, time.Now().UnixMilli()); err != nil {
					return err
				}
				writeByTable[put.TableName] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(put.Item))
				continue
			}

			if item.Delete != nil {
				del := item.Delete
				if err := input.Adapter.ValidateReturnOnFail(del.ReturnOnFail); err != nil {
					return invalid(err.Error())
				}
				t, err := s.getActiveTable(txCtx, repos.Tables(), del.TableName)
				if err != nil {
					return err
				}
				pk, sk, err := model.ExtractKey(t, del.Key)
				if err != nil {
					return invalid(err.Error())
				}
				target := t.Name + "|" + pk + "|" + sk
				if _, exists := seenTargets[target]; exists {
					return invalid("Transaction request cannot include multiple operations on one item")
				}
				seenTargets[target] = struct{}{}

				current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				existed := true
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if errors.Is(err, sql.ErrNoRows) {
					existed = false
					current = map[string]any{}
				}

				ok, err := input.Adapter.EvaluateCondition(del.Condition, current, del.ExprNames, del.ExprValues)
				if err != nil {
					return input.Adapter.OnConditionEvalError(len(input.Items), i, err.Error())
				}
				if !ok {
					return input.Adapter.OnConditionFailed(len(input.Items), i, del.ReturnOnFail, current, existed)
				}
				if err := repos.Items().DeleteItem(txCtx, t.Name, pk, sk); err != nil {
					return err
				}
				if existed {
					if err := input.Adapter.EmitMutation(txCtx, repos, t, "REMOVE", del.Key, current, nil, time.Now().UnixMilli()); err != nil {
						return err
					}
				}
				writeByTable[del.TableName] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(current))
				continue
			}

			if item.Update != nil {
				upd := item.Update
				if err := input.Adapter.ValidateReturnOnFail(upd.ReturnOnFail); err != nil {
					return invalid(err.Error())
				}
				t, err := s.getActiveTable(txCtx, repos.Tables(), upd.TableName)
				if err != nil {
					return err
				}
				pk, sk, err := model.ExtractKey(t, upd.Key)
				if err != nil {
					return invalid(err.Error())
				}
				target := t.Name + "|" + pk + "|" + sk
				if _, exists := seenTargets[target]; exists {
					return invalid("Transaction request cannot include multiple operations on one item")
				}
				seenTargets[target] = struct{}{}

				existing, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				existed := true
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if errors.Is(err, sql.ErrNoRows) {
					existed = false
					existing = map[string]any{}
				}

				current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				if err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						current = map[string]any{t.HashKey: upd.Key[t.HashKey]}
						if t.RangeKey != "" {
							current[t.RangeKey] = upd.Key[t.RangeKey]
						}
					} else {
						return err
					}
				}

				ok, err := input.Adapter.EvaluateCondition(upd.Condition, current, upd.ExprNames, upd.ExprValues)
				if err != nil {
					return input.Adapter.OnConditionEvalError(len(input.Items), i, err.Error())
				}
				if !ok {
					return input.Adapter.OnConditionFailed(len(input.Items), i, upd.ReturnOnFail, existing, existed)
				}

				updated, err := input.Adapter.ApplyUpdate(current, upd.UpdateExpression, upd.ExprNames, upd.ExprValues)
				if err != nil {
					return input.Adapter.OnConditionEvalError(len(input.Items), i, err.Error())
				}
				if err := model.ValidateSecondaryIndexKeyTypes(t, updated); err != nil {
					return input.Adapter.OnConditionEvalError(len(input.Items), i, err.Error())
				}
				if model.ItemTooLarge(updated) {
					return invalid("Item size has exceeded the maximum allowed size")
				}
				if err := repos.Items().PutItem(txCtx, t.Name, pk, sk, updated); err != nil {
					return err
				}

				eventName := "INSERT"
				oldImage := existing
				if existed {
					eventName = "MODIFY"
				} else {
					oldImage = nil
				}
				if err := input.Adapter.EmitMutation(txCtx, repos, t, eventName, upd.Key, oldImage, updated, time.Now().UnixMilli()); err != nil {
					return err
				}
				writeByTable[upd.TableName] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(updated))
				continue
			}

			if item.ConditionCheck != nil {
				cc := item.ConditionCheck
				if err := input.Adapter.ValidateReturnOnFail(cc.ReturnOnFail); err != nil {
					return invalid(err.Error())
				}
				t, err := s.getActiveTable(txCtx, repos.Tables(), cc.TableName)
				if err != nil {
					return err
				}
				pk, sk, err := model.ExtractKey(t, cc.Key)
				if err != nil {
					return invalid(err.Error())
				}
				target := t.Name + "|" + pk + "|" + sk
				if _, exists := seenTargets[target]; exists {
					return invalid("Transaction request cannot include multiple operations on one item")
				}
				seenTargets[target] = struct{}{}

				current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				existed := true
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if errors.Is(err, sql.ErrNoRows) {
					existed = false
					current = map[string]any{}
				}

				ok, err := input.Adapter.EvaluateCondition(cc.Condition, current, cc.ExprNames, cc.ExprValues)
				if err != nil {
					return input.Adapter.OnConditionEvalError(len(input.Items), i, err.Error())
				}
				if !ok {
					return input.Adapter.OnConditionFailed(len(input.Items), i, cc.ReturnOnFail, current, existed)
				}
				continue
			}

			return invalid("each transact item must contain one operation")
		}

		for tableName, units := range writeByTable {
			t, err := s.getActiveTable(txCtx, repos.Tables(), tableName)
			if err != nil {
				return err
			}
			if err := input.Adapter.EnsureWrite(t, units); err != nil {
				return err
			}
		}

		response := input.Adapter.BuildResponse(writeByTable)
		if input.ClientRequestToken != "" {
			hash, err := input.Adapter.RequestHash()
			if err != nil {
				return err
			}
			record := model.TransactWriteIdempotencyRecord{
				Token:       input.ClientRequestToken,
				RequestHash: hash,
				Response:    response,
				CreatedAt:   now,
				ExpiresAt:   now + input.IdempotencyTTLMillis,
			}
			if err := repos.Items().PutTransactWriteIdempotency(txCtx, record); err != nil {
				return err
			}
		}
		result.Response = response
		return nil
	})
	return result, err
}
