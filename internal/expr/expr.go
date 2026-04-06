package expr

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

func Evaluate(condition string, item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return true, nil
	}

	clauses := splitByAND(condition)
	for _, clause := range clauses {
		ok, err := evaluateSingle(strings.TrimSpace(clause), item, names, values)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func evaluateSingle(condition string, item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	if condition == "" {
		return true, nil
	}

	if strings.HasPrefix(condition, "attribute_exists(") && strings.HasSuffix(condition, ")") {
		target, err := resolveAttr(strings.TrimSuffix(strings.TrimPrefix(condition, "attribute_exists("), ")"), names)
		if err != nil {
			return false, err
		}
		_, ok := item[target]
		return ok, nil
	}

	if strings.HasPrefix(condition, "attribute_not_exists(") && strings.HasSuffix(condition, ")") {
		target, err := resolveAttr(strings.TrimSuffix(strings.TrimPrefix(condition, "attribute_not_exists("), ")"), names)
		if err != nil {
			return false, err
		}
		_, ok := item[target]
		return !ok, nil
	}

	if strings.HasPrefix(condition, "begins_with(") && strings.HasSuffix(condition, ")") {
		arg1, arg2, err := parseTwoArgs(strings.TrimSuffix(strings.TrimPrefix(condition, "begins_with("), ")"))
		if err != nil {
			return false, err
		}
		name, err := resolveAttr(arg1, names)
		if err != nil {
			return false, err
		}
		left, ok := item[name]
		if !ok {
			return false, nil
		}
		right, ok := values[strings.TrimSpace(arg2)]
		if !ok {
			return false, fmt.Errorf("missing expression value %q", strings.TrimSpace(arg2))
		}
		ls, lOK := attrString(left)
		rs, rOK := attrString(right)
		if !lOK || !rOK {
			return false, fmt.Errorf("begins_with requires string operands")
		}
		return strings.HasPrefix(ls, rs), nil
	}

	if strings.HasPrefix(condition, "contains(") && strings.HasSuffix(condition, ")") {
		arg1, arg2, err := parseTwoArgs(strings.TrimSuffix(strings.TrimPrefix(condition, "contains("), ")"))
		if err != nil {
			return false, err
		}
		name, err := resolveAttr(arg1, names)
		if err != nil {
			return false, err
		}
		left, ok := item[name]
		if !ok {
			return false, nil
		}
		right, ok := values[strings.TrimSpace(arg2)]
		if !ok {
			return false, fmt.Errorf("missing expression value %q", strings.TrimSpace(arg2))
		}
		ls, lOK := attrString(left)
		rs, rOK := attrString(right)
		if lOK && rOK {
			return strings.Contains(ls, rs), nil
		}
		return false, fmt.Errorf("contains currently supports string operands")
	}

	if strings.HasPrefix(condition, "attribute_type(") && strings.HasSuffix(condition, ")") {
		arg1, arg2, err := parseTwoArgs(strings.TrimSuffix(strings.TrimPrefix(condition, "attribute_type("), ")"))
		if err != nil {
			return false, err
		}
		name, err := resolveAttr(arg1, names)
		if err != nil {
			return false, err
		}
		value, ok := item[name]
		if !ok {
			return false, nil
		}
		expected, ok := values[strings.TrimSpace(arg2)]
		if !ok {
			return false, fmt.Errorf("missing expression value %q", strings.TrimSpace(arg2))
		}
		es, ok := attrString(expected)
		if !ok {
			return false, fmt.Errorf("attribute_type expects string type code")
		}
		return hasAttrType(value, es), nil
	}

	for _, op := range []string{"<=", ">=", "<", ">", "="} {
		if strings.Contains(condition, op) {
			parts := strings.Split(condition, op)
			if len(parts) != 2 {
				return false, fmt.Errorf("invalid comparison expression")
			}
			leftName, err := resolveAttr(parts[0], names)
			if err != nil {
				return false, err
			}
			rightToken := strings.TrimSpace(parts[1])
			right, ok := values[rightToken]
			if !ok {
				return false, fmt.Errorf("missing expression value %q", rightToken)
			}
			left, ok := item[leftName]
			if !ok {
				return false, nil
			}
			return compare(left, right, op)
		}
	}

	return false, fmt.Errorf("unsupported condition expression")
}

func compare(left any, right any, op string) (bool, error) {
	ls, lok := attrString(left)
	rs, rok := attrString(right)
	if lok && rok {
		return compareStrings(ls, rs, op)
	}

	ln, lok := attrNumber(left)
	rn, rok := attrNumber(right)
	if lok && rok {
		return compareNumbers(ln, rn, op)
	}

	if op == "=" {
		return reflect.DeepEqual(left, right), nil
	}
	return false, fmt.Errorf("unsupported operand types for %s", op)
}

func compareStrings(left string, right string, op string) (bool, error) {
	switch op {
	case "=":
		return left == right, nil
	case "<":
		return left < right, nil
	case "<=":
		return left <= right, nil
	case ">":
		return left > right, nil
	case ">=":
		return left >= right, nil
	default:
		return false, fmt.Errorf("unsupported string operator %s", op)
	}
}

func compareNumbers(left float64, right float64, op string) (bool, error) {
	switch op {
	case "=":
		return left == right, nil
	case "<":
		return left < right, nil
	case "<=":
		return left <= right, nil
	case ">":
		return left > right, nil
	case ">=":
		return left >= right, nil
	default:
		return false, fmt.Errorf("unsupported number operator %s", op)
	}
}

func parseTwoArgs(raw string) (string, string, error) {
	parts := strings.Split(raw, ",")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected two arguments")
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

func splitByAND(expr string) []string {
	parts := []string{}
	depth := 0
	start := 0
	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && i+3 <= len(expr) {
				segment := expr[i : i+3]
				if strings.EqualFold(segment, "AND") {
					leftBoundary := i == 0 || unicode.IsSpace(rune(expr[i-1]))
					rightBoundary := i+3 >= len(expr) || unicode.IsSpace(rune(expr[i+3]))
					if leftBoundary && rightBoundary {
						parts = append(parts, strings.TrimSpace(expr[start:i]))
						start = i + 3
						i += 2
					}
				}
			}
		}
	}
	parts = append(parts, strings.TrimSpace(expr[start:]))
	return parts
}

func attrString(v any) (string, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return "", false
	}
	s, ok := m["S"].(string)
	if !ok {
		return "", false
	}
	return s, true
}

func attrNumber(v any) (float64, bool) {
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

func hasAttrType(v any, typ string) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}
	_, ok = m[typ]
	if ok {
		return true
	}
	if typ == "M" || typ == "L" {
		_, err := json.Marshal(m)
		return err == nil
	}
	return false
}

func resolveAttr(token string, names map[string]string) (string, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", fmt.Errorf("empty attribute name")
	}
	if strings.HasPrefix(token, "#") {
		r, ok := names[token]
		if !ok {
			return "", fmt.Errorf("missing expression name %q", token)
		}
		return r, nil
	}
	return token, nil
}
