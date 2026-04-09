package httpapi

import (
	"fmt"
	"strconv"
	"strings"

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
	s = strings.TrimSpace(s)
	if s == "" {
		return pk, nil, fmt.Errorf("KeyConditionExpression is required")
	}

	idx := findTopLevelAND(s)
	pkExpr := s
	var skExpr string
	if idx >= 0 {
		pkExpr = strings.TrimSpace(s[:idx])
		skExpr = strings.TrimSpace(s[idx+3:])
	}

	pk, err = parseSingleEq(pkExpr)
	if err != nil {
		return pk, nil, err
	}

	if strings.TrimSpace(skExpr) != "" {
		parsed, err := parseSortCondition(skExpr)
		if err != nil {
			return pk, nil, err
		}
		sk = &parsed
	}
	return pk, sk, nil
}

func findTopLevelAND(expr string) int {
	depth := 0
	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && i+3 <= len(expr) && strings.EqualFold(expr[i:i+3], "AND") {
				leftBoundary := i == 0 || expr[i-1] == ' '
				rightBoundary := i+3 >= len(expr) || expr[i+3] == ' '
				if leftBoundary && rightBoundary {
					return i
				}
			}
		}
	}
	return -1
}

func parseSingleEq(s string) (keyExprToken, error) {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return keyExprToken{}, fmt.Errorf("partition key condition must use '='")
	}
	a := strings.TrimSpace(parts[0])
	v := strings.TrimSpace(parts[1])
	if a == "" || v == "" {
		return keyExprToken{}, fmt.Errorf("invalid key condition segment")
	}
	return keyExprToken{attr: a, value: v}, nil
}

func parseSortCondition(s string) (sortKeyCondition, error) {
	s = strings.TrimSpace(s)

	if strings.HasPrefix(strings.ToLower(s), "begins_with(") && strings.HasSuffix(s, ")") {
		inside := s[len("begins_with(") : len(s)-1]
		parts := strings.Split(inside, ",")
		if len(parts) != 2 {
			return sortKeyCondition{}, fmt.Errorf("invalid begins_with sort key condition")
		}
		return sortKeyCondition{attr: strings.TrimSpace(parts[0]), op: "begins_with", value1: strings.TrimSpace(parts[1])}, nil
	}

	if idx := strings.Index(strings.ToUpper(s), " BETWEEN "); idx >= 0 {
		attr := strings.TrimSpace(s[:idx])
		right := strings.TrimSpace(s[idx+len(" BETWEEN "):])
		andIdx := strings.Index(strings.ToUpper(right), " AND ")
		if andIdx < 0 {
			return sortKeyCondition{}, fmt.Errorf("invalid BETWEEN sort key condition")
		}
		v1 := strings.TrimSpace(right[:andIdx])
		v2 := strings.TrimSpace(right[andIdx+len(" AND "):])
		if v1 == "" || v2 == "" {
			return sortKeyCondition{}, fmt.Errorf("BETWEEN requires two values")
		}
		return sortKeyCondition{attr: attr, op: "BETWEEN", value1: v1, value2: v2}, nil
	}

	for _, op := range []string{"<=", ">=", "<", ">", "="} {
		parts := strings.Split(s, op)
		if len(parts) == 2 {
			return sortKeyCondition{attr: strings.TrimSpace(parts[0]), op: op, value1: strings.TrimSpace(parts[1])}, nil
		}
	}
	return sortKeyCondition{}, fmt.Errorf("unsupported sort key condition")
}

func sortConditionMatches(item map[string]any, cond *sortKeyCondition, names map[string]string, values map[string]any) (bool, error) {
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

	left, err := model.SerializeKeyValue(raw)
	if err != nil {
		return false, err
	}

	v1, ok := values[cond.value1]
	if !ok {
		return false, fmt.Errorf("missing sort key expression value %q", cond.value1)
	}
	right1, err := model.SerializeKeyValue(v1)
	if err != nil {
		return false, err
	}

	switch cond.op {
	case "=":
		return left == right1, nil
	case "<":
		return left < right1, nil
	case "<=":
		return left <= right1, nil
	case ">":
		return left > right1, nil
	case ">=":
		return left >= right1, nil
	case "begins_with":
		return strings.HasPrefix(left, right1), nil
	case "BETWEEN":
		v2, ok := values[cond.value2]
		if !ok {
			return false, fmt.Errorf("missing sort key expression value %q", cond.value2)
		}
		right2, err := model.SerializeKeyValue(v2)
		if err != nil {
			return false, err
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
