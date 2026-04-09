package httpapi

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pinax/internal/expr"
)

type updatePlan struct {
	Set          map[string]any
	Remove       []string
	Add          map[string]any
	TouchedAttrs map[string]struct{}
}

func parseUpdateExpression(raw string, names map[string]string, values map[string]any) (updatePlan, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return updatePlan{}, fmt.Errorf("UpdateExpression is required")
	}

	plan := updatePlan{Set: map[string]any{}, Add: map[string]any{}, Remove: []string{}, TouchedAttrs: map[string]struct{}{}}
	upper := strings.ToUpper(raw)

	sections := []struct {
		keyword string
		nextA   string
		nextB   string
	}{
		{keyword: "SET", nextA: "REMOVE", nextB: "ADD"},
		{keyword: "REMOVE", nextA: "SET", nextB: "ADD"},
		{keyword: "ADD", nextA: "SET", nextB: "REMOVE"},
	}

	for _, section := range sections {
		start := strings.Index(upper, section.keyword+" ")
		if start < 0 {
			continue
		}
		contentStart := start + len(section.keyword) + 1
		contentEnd := len(raw)
		for _, nextKeyword := range []string{section.nextA, section.nextB} {
			next := strings.Index(upper[contentStart:], " "+nextKeyword+" ")
			if next >= 0 {
				candidate := contentStart + next + 1
				if candidate < contentEnd {
					contentEnd = candidate
				}
			}
		}
		content := strings.TrimSpace(raw[contentStart:contentEnd])
		if content == "" {
			continue
		}

		switch section.keyword {
		case "SET":
			assignments := splitTopLevel(content, ',')
			for _, assignment := range assignments {
				parts := strings.SplitN(assignment, "=", 2)
				if len(parts) != 2 {
					return updatePlan{}, fmt.Errorf("invalid SET clause")
				}
				attr, err := resolveNameStrict(strings.TrimSpace(parts[0]), names)
				if err != nil {
					return updatePlan{}, err
				}
				rhs := strings.TrimSpace(parts[1])
				if rhs == "" {
					return updatePlan{}, fmt.Errorf("Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"= \"")
				}
				value, err := evalSetValue(rhs, attr, names, values)
				if err != nil {
					return updatePlan{}, err
				}
				plan.Set[attr] = value
				plan.TouchedAttrs[attr] = struct{}{}
			}
		case "REMOVE":
			attrs := splitTopLevel(content, ',')
			for _, attr := range attrs {
				resolved, err := resolveNameStrict(strings.TrimSpace(attr), names)
				if err != nil {
					return updatePlan{}, err
				}
				if resolved == "" {
					continue
				}
				plan.Remove = append(plan.Remove, resolved)
				plan.TouchedAttrs[resolved] = struct{}{}
			}
		case "ADD":
			parts := splitTopLevel(content, ',')
			for _, entry := range parts {
				fields := strings.Fields(entry)
				if len(fields) != 2 {
					return updatePlan{}, fmt.Errorf("invalid ADD clause")
				}
				attr, err := resolveNameStrict(fields[0], names)
				if err != nil {
					return updatePlan{}, err
				}
				v, ok := values[fields[1]]
				if !ok {
					return updatePlan{}, fmt.Errorf("Invalid UpdateExpression: An expression attribute value used in expression is not defined; attribute value: %s", fields[1])
				}
				plan.Add[attr] = v
				plan.TouchedAttrs[attr] = struct{}{}
			}
		}
	}

	if len(plan.Set) == 0 && len(plan.Remove) == 0 && len(plan.Add) == 0 {
		return updatePlan{}, fmt.Errorf("only SET/REMOVE/ADD update expressions are supported")
	}
	return plan, nil
}

func evalSetValue(raw string, attrName string, names map[string]string, values map[string]any) (any, error) {
	raw = strings.TrimSpace(raw)
	lowerRaw := strings.ToLower(raw)

	if strings.HasPrefix(lowerRaw, "list_append(") && strings.HasSuffix(raw, ")") {
		inside := strings.TrimSpace(raw[len("list_append(") : len(raw)-1])
		arg1, arg2, err := parseTwoArgs(inside)
		if err != nil {
			return nil, err
		}
		left, err := evalListOperand(arg1, names, values)
		if err != nil {
			return nil, err
		}
		right, err := evalListOperand(arg2, names, values)
		if err != nil {
			return nil, err
		}
		return map[string]any{"__list_append_left": left, "__list_append_right": right}, nil
	}

	if strings.HasPrefix(strings.ToLower(raw), "if_not_exists(") && strings.HasSuffix(raw, ")") {
		inside := strings.TrimSuffix(strings.TrimPrefix(raw, "if_not_exists("), ")")
		arg1, arg2, err := parseTwoArgs(inside)
		if err != nil {
			return nil, err
		}
		target, err := resolveNameStrict(arg1, names)
		if err != nil {
			return nil, err
		}
		if target == "" {
			return nil, fmt.Errorf("invalid if_not_exists target")
		}
		v, ok := values[strings.TrimSpace(arg2)]
		if !ok {
			return nil, fmt.Errorf("Invalid UpdateExpression: An expression attribute value used in expression is not defined; attribute value: %s", strings.TrimSpace(arg2))
		}
		return map[string]any{"__if_not_exists_attr": target, "__if_not_exists_default": v}, nil
	}

	for _, op := range []string{"+", "-"} {
		parts := splitByOperatorTopLevel(raw, op)
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])
			resolvedLeft, err := resolveNameStrict(left, names)
			if err != nil {
				return nil, err
			}
			if resolvedLeft == attrName {
				v, ok := values[right]
				if !ok {
					return nil, fmt.Errorf("Invalid UpdateExpression: An expression attribute value used in expression is not defined; attribute value: %s", right)
				}
				return map[string]any{"__arith_op": op, "__arith_value": v}, nil
			}
		}
	}

	v, ok := values[raw]
	if !ok {
		return nil, fmt.Errorf("Invalid UpdateExpression: An expression attribute value used in expression is not defined; attribute value: %s", raw)
	}
	return v, nil
}

func applyUpdatePlan(current map[string]any, plan updatePlan) (map[string]any, map[string]any, error) {
	next := cloneItem(current)
	changed := map[string]any{}

	for attr, v := range plan.Set {
		if m, ok := v.(map[string]any); ok {
			if _, ok := m["__list_append_left"]; ok {
				result, err := applyListAppend(next, m)
				if err != nil {
					return nil, nil, err
				}
				next[attr] = result
				changed[attr] = result
				continue
			}
			if fallbackAttr, ok := m["__if_not_exists_attr"].(string); ok {
				if _, exists := next[fallbackAttr]; !exists {
					next[attr] = m["__if_not_exists_default"]
					changed[attr] = next[attr]
				}
				continue
			}
			if arithOp, ok := m["__arith_op"].(string); ok {
				result, err := applyArithmetic(next[attr], m["__arith_value"], arithOp)
				if err != nil {
					return nil, nil, err
				}
				next[attr] = result
				changed[attr] = result
				continue
			}
		}
		next[attr] = v
		changed[attr] = v
	}

	for _, attr := range plan.Remove {
		delete(next, attr)
		changed[attr] = nil
	}

	for attr, delta := range plan.Add {
		result, err := applyArithmetic(next[attr], delta, "+")
		if err != nil {
			return nil, nil, err
		}
		next[attr] = result
		changed[attr] = result
	}

	return next, changed, nil
}

func evalListOperand(raw string, names map[string]string, values map[string]any) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if v, ok := values[raw]; ok {
		return map[string]any{"__literal": v}, nil
	}
	if strings.HasPrefix(strings.ToLower(raw), "if_not_exists(") && strings.HasSuffix(raw, ")") {
		inside := strings.TrimSuffix(strings.TrimPrefix(raw, "if_not_exists("), ")")
		arg1, arg2, err := parseTwoArgs(inside)
		if err != nil {
			return nil, err
		}
		target, err := resolveNameStrict(arg1, names)
		if err != nil {
			return nil, err
		}
		if target == "" {
			return nil, fmt.Errorf("invalid if_not_exists target")
		}
		v, ok := values[strings.TrimSpace(arg2)]
		if !ok {
			return nil, fmt.Errorf("Invalid UpdateExpression: An expression attribute value used in expression is not defined; attribute value: %s", strings.TrimSpace(arg2))
		}
		return map[string]any{"__if_not_exists_attr": target, "__if_not_exists_default": v}, nil
	}
	resolved, err := resolveNameStrict(raw, names)
	if err != nil {
		return nil, err
	}
	return map[string]any{"__attr_ref": resolved}, nil
}

func applyListAppend(next map[string]any, plan map[string]any) (map[string]any, error) {
	left, err := resolveListOperand(next, plan["__list_append_left"])
	if err != nil {
		return nil, err
	}
	right, err := resolveListOperand(next, plan["__list_append_right"])
	if err != nil {
		return nil, err
	}
	combined := make([]any, 0, len(left)+len(right))
	combined = append(combined, left...)
	combined = append(combined, right...)
	return map[string]any{"L": combined}, nil
}

func resolveListOperand(current map[string]any, operand any) ([]any, error) {
	m, ok := operand.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("list_append operands must be list attributes or list values")
	}

	if attr, ok := m["__attr_ref"].(string); ok {
		v, exists := current[attr]
		if !exists {
			return nil, fmt.Errorf("list_append requires existing list attribute %q", attr)
		}
		return asList(v)
	}

	if attr, ok := m["__if_not_exists_attr"].(string); ok {
		if v, exists := current[attr]; exists {
			return asList(v)
		}
		return asList(m["__if_not_exists_default"])
	}

	if literal, ok := m["__literal"]; ok {
		return asList(literal)
	}

	return nil, fmt.Errorf("list_append operands must be list attributes or list values")
}

func asList(v any) ([]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("list_append supports only L operands")
	}
	list, ok := m["L"].([]any)
	if !ok {
		return nil, fmt.Errorf("list_append supports only L operands")
	}
	out := make([]any, len(list))
	copy(out, list)
	return out, nil
}

func applyArithmetic(existing any, delta any, op string) (any, error) {
	current := 0.0
	if existing != nil {
		n, ok := exprNumber(existing)
		if !ok {
			return nil, fmt.Errorf("arithmetic updates require N attributes")
		}
		current = n
	}
	change, ok := exprNumber(delta)
	if !ok {
		return nil, fmt.Errorf("arithmetic updates require N expression values")
	}
	switch op {
	case "+":
		current += change
	case "-":
		current -= change
	default:
		return nil, fmt.Errorf("unsupported arithmetic operator %s", op)
	}
	return map[string]any{"N": trimTrailingZeros(current)}, nil
}

func applyProjection(item map[string]any, projectionExpression string, names map[string]string) (map[string]any, error) {
	if strings.TrimSpace(projectionExpression) == "" {
		return cloneItem(item), nil
	}
	out := map[string]any{}
	for _, token := range splitTopLevel(projectionExpression, ',') {
		attr, err := resolveNameStrict(strings.TrimSpace(token), names)
		if err != nil {
			return nil, err
		}
		if attr == "" {
			continue
		}
		if v, ok := item[attr]; ok {
			out[attr] = v
		}
	}
	return out, nil
}

func applyFilter(item map[string]any, filterExpression string, names map[string]string, values map[string]any) (bool, error) {
	if strings.TrimSpace(filterExpression) == "" {
		return true, nil
	}
	return expr.Evaluate(filterExpression, item, names, values)
}

func cloneItem(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func splitTopLevel(raw string, sep rune) []string {
	parts := []string{}
	start := 0
	depth := 0
	for i, r := range raw {
		switch r {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case sep:
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(raw[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(raw[start:]))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func splitByOperatorTopLevel(raw string, op string) []string {
	depth := 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && i+len(op) <= len(raw) && raw[i:i+len(op)] == op {
				return []string{raw[:i], raw[i+len(op):]}
			}
		}
	}
	return nil
}

func parseTwoArgs(raw string) (string, string, error) {
	parts := splitTopLevel(raw, ',')
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected two arguments")
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

func exprNumber(v any) (float64, bool) {
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

func trimTrailingZeros(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
