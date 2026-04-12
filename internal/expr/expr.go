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
	return evaluateParsed(condition, item, names, values)
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

func attrBinary(v any) (string, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return "", false
	}
	b, ok := m["B"].(string)
	if !ok {
		return "", false
	}
	return b, true
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
	if !strings.Contains(token, "#") {
		return token, nil
	}
	segments := strings.Split(token, ".")
	resolved := make([]string, 0, len(segments))
	for _, seg := range segments {
		name, indexes := parsePathSegment(seg)
		if strings.HasPrefix(name, "#") {
			r, ok := names[name]
			if !ok {
				return "", fmt.Errorf("missing expression name %q", name)
			}
			name = r
		}
		if name == "" {
			return "", fmt.Errorf("empty attribute name")
		}
		var b strings.Builder
		b.WriteString(name)
		for _, idx := range indexes {
			b.WriteString("[")
			b.WriteString(strconv.Itoa(idx))
			b.WriteString("]")
		}
		resolved = append(resolved, b.String())
	}
	return strings.Join(resolved, "."), nil
}
