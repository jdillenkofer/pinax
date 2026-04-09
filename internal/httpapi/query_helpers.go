package httpapi

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pinax/internal/expr"
	"github.com/jdillenkofer/pinax/internal/model"
)

type keyExprToken struct {
	attr  string
	value string
}

type sortKeyCondition struct {
	attr   string
	op     string
	value1 string
	value2 string
}

func parseKeyCondition(s string) (pk keyExprToken, sk *sortKeyCondition, err error) {
	parsed, err := expr.ParseKeyCondition(s)
	if err != nil {
		return pk, nil, err
	}
	pk = keyExprToken{attr: parsed.Partition.Attribute, value: parsed.Partition.Value1}
	if parsed.Sort != nil {
		sk = &sortKeyCondition{attr: parsed.Sort.Attribute, op: parsed.Sort.Operator, value1: parsed.Sort.Value1, value2: parsed.Sort.Value2}
	}
	return pk, sk, nil
}

func sortConditionMatches(item map[string]any, cond *sortKeyCondition, names map[string]string, values map[string]any, expectedType string) (bool, error) {
	if cond == nil {
		return true, nil
	}
	attr, err := resolveNameStrict(cond.attr, names)
	if err != nil {
		return false, err
	}
	raw, ok := item[attr]
	if !ok {
		return false, nil
	}
	if err := model.ValidateKeyAttributeType(raw, expectedType, attr); err != nil {
		return false, err
	}

	left, err := model.SerializeKeyValue(raw)
	if err != nil {
		return false, err
	}

	v1, ok := values[cond.value1]
	if !ok {
		return false, fmt.Errorf("missing sort key expression value %q", cond.value1)
	}
	if err := model.ValidateKeyAttributeType(v1, expectedType, attr); err != nil {
		return false, err
	}
	right1, err := model.SerializeKeyValue(v1)
	if err != nil {
		return false, err
	}
	leftNum, leftIsNum := numberFromN(raw)
	rightNum, rightIsNum := numberFromN(v1)

	switch cond.op {
	case "=":
		if leftIsNum && rightIsNum {
			return leftNum == rightNum, nil
		}
		return left == right1, nil
	case "<":
		if leftIsNum && rightIsNum {
			return leftNum < rightNum, nil
		}
		return left < right1, nil
	case "<=":
		if leftIsNum && rightIsNum {
			return leftNum <= rightNum, nil
		}
		return left <= right1, nil
	case ">":
		if leftIsNum && rightIsNum {
			return leftNum > rightNum, nil
		}
		return left > right1, nil
	case ">=":
		if leftIsNum && rightIsNum {
			return leftNum >= rightNum, nil
		}
		return left >= right1, nil
	case "begins_with":
		return strings.HasPrefix(left, right1), nil
	case "BETWEEN":
		v2, ok := values[cond.value2]
		if !ok {
			return false, fmt.Errorf("missing sort key expression value %q", cond.value2)
		}
		if err := model.ValidateKeyAttributeType(v2, expectedType, attr); err != nil {
			return false, err
		}
		right2, err := model.SerializeKeyValue(v2)
		if err != nil {
			return false, err
		}
		if leftIsNum && rightIsNum {
			rightNum2, ok := numberFromN(v2)
			if ok {
				return leftNum >= rightNum && leftNum <= rightNum2, nil
			}
		}
		return left >= right1 && left <= right2, nil
	default:
		return false, fmt.Errorf("unsupported sort key operator %s", cond.op)
	}
}

func parseLimit(limit int) int {
	if limit <= 0 {
		return 0
	}
	return limit
}

func keyFromItem(table model.Table, item map[string]any) map[string]any {
	key := map[string]any{table.HashKey: item[table.HashKey]}
	if table.RangeKey != "" {
		key[table.RangeKey] = item[table.RangeKey]
	}
	return key
}

func compareSKForDirection(a, b string, scanForward bool) int {
	if a == b {
		return 0
	}
	if scanForward {
		if a < b {
			return -1
		}
		return 1
	}
	if a > b {
		return -1
	}
	return 1
}

func numberFromN(v any) (float64, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return 0, false
	}
	n, ok := m["N"].(string)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(n, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}
