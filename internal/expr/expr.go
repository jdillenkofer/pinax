package expr

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func Evaluate(condition string, item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return true, nil
	}
	return evalBoolean(condition, item, names, values)
}

func evalBoolean(condition string, item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	if condition == "" {
		return true, nil
	}
	condition = strings.TrimSpace(condition)

	if inner, ok := trimOuterParens(condition); ok {
		return evalBoolean(inner, item, names, values)
	}

	if idx := findTopLevelKeyword(condition, "OR"); idx >= 0 {
		left := strings.TrimSpace(condition[:idx])
		right := strings.TrimSpace(condition[idx+2:])
		l, err := evalBoolean(left, item, names, values)
		if err != nil {
			return false, err
		}
		if l {
			return true, nil
		}
		return evalBoolean(right, item, names, values)
	}

	if idx := findTopLevelKeyword(condition, "AND"); idx >= 0 {
		left := strings.TrimSpace(condition[:idx])
		right := strings.TrimSpace(condition[idx+3:])
		l, err := evalBoolean(left, item, names, values)
		if err != nil {
			return false, err
		}
		if !l {
			return false, nil
		}
		return evalBoolean(right, item, names, values)
	}

	if strings.HasPrefix(strings.ToUpper(condition), "NOT ") {
		inner := strings.TrimSpace(condition[4:])
		ok, err := evalBoolean(inner, item, names, values)
		if err != nil {
			return false, err
		}
		return !ok, nil
	}

	return evaluateSingle(condition, item, names, values)
}

func evaluateSingle(condition string, item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	if inner, ok := trimOuterParens(condition); ok {
		return evaluateSingle(inner, item, names, values)
	}

	if strings.HasPrefix(condition, "attribute_exists(") && strings.HasSuffix(condition, ")") {
		target, err := resolveAttr(strings.TrimSuffix(strings.TrimPrefix(condition, "attribute_exists("), ")"), names)
		if err != nil {
			return false, err
		}
		_, ok := getPathValue(item, target)
		return ok, nil
	}

	if strings.HasPrefix(condition, "attribute_not_exists(") && strings.HasSuffix(condition, ")") {
		target, err := resolveAttr(strings.TrimSuffix(strings.TrimPrefix(condition, "attribute_not_exists("), ")"), names)
		if err != nil {
			return false, err
		}
		_, ok := getPathValue(item, target)
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
		left, ok := getPathValue(item, name)
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
		left, ok := getPathValue(item, name)
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
		value, ok := getPathValue(item, name)
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

	upper := strings.ToUpper(condition)
	if idx := strings.Index(upper, " IN "); idx >= 0 {
		leftToken := strings.TrimSpace(condition[:idx])
		rightToken := strings.TrimSpace(condition[idx+4:])
		if !strings.HasPrefix(rightToken, "(") || !strings.HasSuffix(rightToken, ")") {
			return false, fmt.Errorf("IN requires parenthesized values")
		}
		leftName, err := resolveAttr(leftToken, names)
		if err != nil {
			return false, err
		}
		left, ok := getPathValue(item, leftName)
		if !ok {
			return false, nil
		}
		vals := splitValues(strings.TrimSpace(rightToken[1 : len(rightToken)-1]))
		for _, token := range vals {
			v, ok := values[strings.TrimSpace(token)]
			if !ok {
				return false, fmt.Errorf("missing expression value %q", strings.TrimSpace(token))
			}
			match, err := compare(left, v, "=")
			if err != nil {
				return false, err
			}
			if match {
				return true, nil
			}
		}
		return false, nil
	}

	if strings.Contains(condition, "<>") {
		parts := strings.Split(condition, "<>")
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
		left, ok := getPathValue(item, leftName)
		if !ok {
			return false, nil
		}
		eq, err := compare(left, right, "=")
		if err != nil {
			return false, err
		}
		return !eq, nil
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
			left, ok := getPathValue(item, leftName)
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
	parts := splitValues(raw)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected two arguments")
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

func splitValues(raw string) []string {
	parts := []string{}
	depth := 0
	start := 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(raw[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(raw[start:]))
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
	actual := attrType(v)
	if actual == "" {
		return false
	}
	return actual == typ
}

func attrType(v any) string {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return ""
	}
	for k := range m {
		return k
	}
	return ""
}

func trimOuterParens(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return "", false
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return "", false
			}
		}
	}
	if depth != 0 {
		return "", false
	}
	return strings.TrimSpace(s[1 : len(s)-1]), true
}

func findTopLevelKeyword(s string, keyword string) int {
	depth := 0
	upper := strings.ToUpper(s)
	for i := 0; i+len(keyword) <= len(upper); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}
		if upper[i:i+len(keyword)] != keyword {
			continue
		}
		leftBoundary := i == 0 || s[i-1] == ' '
		rightBoundary := i+len(keyword) == len(s) || s[i+len(keyword)] == ' '
		if leftBoundary && rightBoundary {
			return i
		}
	}
	return -1
}

func getPathValue(item map[string]any, path string) (any, bool) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, false
	}
	segments := strings.Split(path, ".")
	var current any
	var ok bool
	current, ok = item[segments[0]]
	if !ok {
		return nil, false
	}
	for _, seg := range segments[1:] {
		next, exists := descendOne(current, seg)
		if !exists {
			return nil, false
		}
		current = next
	}
	return current, true
}

func descendOne(v any, seg string) (any, bool) {
	attr, indexes := parsePathSegment(seg)
	m, ok := v.(map[string]any)
	if !ok {
		return nil, false
	}
	mapValue, ok := m["M"].(map[string]any)
	if !ok {
		return nil, false
	}
	next, ok := mapValue[attr]
	if !ok {
		return nil, false
	}
	for _, index := range indexes {
		lm, ok := next.(map[string]any)
		if !ok {
			return nil, false
		}
		list, ok := lm["L"].([]any)
		if !ok || index < 0 || index >= len(list) {
			return nil, false
		}
		next = list[index]
	}
	return next, true
}

func parsePathSegment(seg string) (string, []int) {
	seg = strings.TrimSpace(seg)
	if !strings.Contains(seg, "[") {
		return seg, nil
	}
	attr := seg
	if idx := strings.Index(seg, "["); idx >= 0 {
		attr = seg[:idx]
	}
	indexes := []int{}
	rest := seg[len(attr):]
	for len(rest) > 0 {
		if !strings.HasPrefix(rest, "[") {
			break
		}
		end := strings.Index(rest, "]")
		if end <= 1 {
			break
		}
		idx, err := strconv.Atoi(rest[1:end])
		if err != nil {
			break
		}
		indexes = append(indexes, idx)
		rest = rest[end+1:]
	}
	return strings.TrimSpace(attr), indexes
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
