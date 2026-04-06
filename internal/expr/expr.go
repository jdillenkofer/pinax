package expr

import (
	"fmt"
	"reflect"
	"strings"
)

func Evaluate(condition string, item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	condition = strings.TrimSpace(condition)
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

	parts := strings.Split(condition, "=")
	if len(parts) == 2 {
		left, err := resolveAttr(parts[0], names)
		if err != nil {
			return false, err
		}
		right := strings.TrimSpace(parts[1])
		v, ok := values[right]
		if !ok {
			return false, fmt.Errorf("missing expression value %q", right)
		}
		current, ok := item[left]
		if !ok {
			return false, nil
		}
		return reflect.DeepEqual(current, v), nil
	}

	return false, fmt.Errorf("unsupported condition expression")
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
