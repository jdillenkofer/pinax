package httpapi

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pinax/internal/expr"
)

type updatePlan struct {
	Set          []setAction
	Remove       []documentPath
	Add          []addAction
	Delete       []deleteAction
	TouchedAttrs map[string]struct{}
}

type setAction struct {
	target documentPath
	value  setValueExpr
}

type addAction struct {
	target documentPath
	value  any
}

type deleteAction struct {
	target documentPath
	value  any
}

type setValueExpr interface {
	resolve(item map[string]any) (any, error)
}

type valueLiteralExpr struct{ value any }

func (e valueLiteralExpr) resolve(item map[string]any) (any, error) { return e.value, nil }

type pathValueExpr struct{ path documentPath }

func (e pathValueExpr) resolve(item map[string]any) (any, error) {
	v, ok, err := getAtPath(item, e.path)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("The provided expression refers to an attribute that does not exist in the item")
	}
	return v, nil
}

type ifNotExistsExpr struct {
	path       documentPath
	defaultVal setValueExpr
}

func (e ifNotExistsExpr) resolve(item map[string]any) (any, error) {
	v, ok, err := getAtPath(item, e.path)
	if err != nil {
		return nil, err
	}
	if ok {
		return v, nil
	}
	return e.defaultVal.resolve(item)
}

type listAppendExpr struct {
	left  setValueExpr
	right setValueExpr
}

func (e listAppendExpr) resolve(item map[string]any) (any, error) {
	left, err := e.left.resolve(item)
	if err != nil {
		return nil, err
	}
	right, err := e.right.resolve(item)
	if err != nil {
		return nil, err
	}
	ll, err := asList(left)
	if err != nil {
		return nil, err
	}
	rl, err := asList(right)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(ll)+len(rl))
	out = append(out, ll...)
	out = append(out, rl...)
	return map[string]any{"L": out}, nil
}

type arithmeticExpr struct {
	left  setValueExpr
	right setValueExpr
	op    string
}

func (e arithmeticExpr) resolve(item map[string]any) (any, error) {
	left, err := e.left.resolve(item)
	if err != nil {
		return nil, err
	}
	right, err := e.right.resolve(item)
	if err != nil {
		return nil, err
	}
	ln, ok := exprNumber(left)
	if !ok {
		return nil, fmt.Errorf("arithmetic updates require N attributes")
	}
	rn, ok := exprNumber(right)
	if !ok {
		return nil, fmt.Errorf("arithmetic updates require N expression values")
	}
	switch e.op {
	case "+":
		ln += rn
	case "-":
		ln -= rn
	default:
		return nil, fmt.Errorf("unsupported arithmetic operator %s", e.op)
	}
	return map[string]any{"N": trimTrailingZeros(ln)}, nil
}

type documentPath struct {
	segments []pathSegment
}

type pathSegment struct {
	name    string
	indexes []int
}

func isTopLevelPath(p documentPath) bool {
	return len(p.segments) == 1 && len(p.segments[0].indexes) == 0
}

func hasPathOverlap(plan updatePlan) bool {
	paths := []documentPath{}
	for _, a := range plan.Set {
		paths = append(paths, a.target)
	}
	for _, p := range plan.Remove {
		paths = append(paths, p)
	}
	for _, a := range plan.Add {
		paths = append(paths, a.target)
	}
	for _, a := range plan.Delete {
		paths = append(paths, a.target)
	}
	for i := 0; i < len(paths); i++ {
		for j := i + 1; j < len(paths); j++ {
			if documentPathsOverlap(paths[i], paths[j]) {
				return true
			}
		}
	}
	return false
}

func documentPathsOverlap(a documentPath, b documentPath) bool {
	minLen := len(a.segments)
	if len(b.segments) < minLen {
		minLen = len(b.segments)
	}
	if minLen == 0 {
		return false
	}
	for i := 0; i < minLen; i++ {
		as := a.segments[i]
		bs := b.segments[i]
		if as.name != bs.name {
			return false
		}
		if len(as.indexes) != len(bs.indexes) {
			prefixLen := len(as.indexes)
			if len(bs.indexes) < prefixLen {
				prefixLen = len(bs.indexes)
			}
			for k := 0; k < prefixLen; k++ {
				if as.indexes[k] != bs.indexes[k] {
					return false
				}
			}
			return true
		}
		for k := 0; k < len(as.indexes); k++ {
			if as.indexes[k] != bs.indexes[k] {
				return false
			}
		}
	}
	return true
}

func (p documentPath) topLevel() string {
	if len(p.segments) == 0 {
		return ""
	}
	return p.segments[0].name
}

func parseUpdateExpression(raw string, names map[string]string, values map[string]any) (updatePlan, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return updatePlan{}, fmt.Errorf("UpdateExpression is required")
	}

	clauses, err := parseUpdateClauses(raw)
	if err != nil {
		return updatePlan{}, err
	}

	plan := updatePlan{TouchedAttrs: map[string]struct{}{}}

	if setRaw, ok := clauses["SET"]; ok {
		items := splitTopLevel(setRaw, ',')
		for _, item := range items {
			parts := strings.SplitN(item, "=", 2)
			if len(parts) != 2 {
				return updatePlan{}, fmt.Errorf("invalid SET clause")
			}
			target, err := parseDocumentPath(parts[0], names)
			if err != nil {
				return updatePlan{}, err
			}
			rhs := strings.TrimSpace(parts[1])
			if rhs == "" {
				return updatePlan{}, fmt.Errorf("Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"= \"")
			}
			valueExpr, err := parseSetValueExpr(rhs, names, values)
			if err != nil {
				return updatePlan{}, err
			}
			plan.Set = append(plan.Set, setAction{target: target, value: valueExpr})
			plan.TouchedAttrs[target.topLevel()] = struct{}{}
		}
	}

	if removeRaw, ok := clauses["REMOVE"]; ok {
		items := splitTopLevel(removeRaw, ',')
		for _, item := range items {
			path, err := parseDocumentPath(item, names)
			if err != nil {
				return updatePlan{}, err
			}
			plan.Remove = append(plan.Remove, path)
			plan.TouchedAttrs[path.topLevel()] = struct{}{}
		}
	}

	if addRaw, ok := clauses["ADD"]; ok {
		items := splitTopLevel(addRaw, ',')
		for _, item := range items {
			fields := strings.Fields(item)
			if len(fields) != 2 {
				return updatePlan{}, fmt.Errorf("invalid ADD clause")
			}
			target, err := parseDocumentPath(fields[0], names)
			if err != nil {
				return updatePlan{}, err
			}
			if !isTopLevelPath(target) {
				return updatePlan{}, fmt.Errorf("Invalid UpdateExpression: The document path provided in the update expression is invalid for update")
			}
			v, err := lookupExpressionValue(fields[1], values)
			if err != nil {
				return updatePlan{}, err
			}
			plan.Add = append(plan.Add, addAction{target: target, value: v})
			plan.TouchedAttrs[target.topLevel()] = struct{}{}
		}
	}

	if deleteRaw, ok := clauses["DELETE"]; ok {
		items := splitTopLevel(deleteRaw, ',')
		for _, item := range items {
			fields := strings.Fields(item)
			if len(fields) != 2 {
				return updatePlan{}, fmt.Errorf("invalid DELETE clause")
			}
			target, err := parseDocumentPath(fields[0], names)
			if err != nil {
				return updatePlan{}, err
			}
			if !isTopLevelPath(target) {
				return updatePlan{}, fmt.Errorf("Invalid UpdateExpression: The document path provided in the update expression is invalid for update")
			}
			v, err := lookupExpressionValue(fields[1], values)
			if err != nil {
				return updatePlan{}, err
			}
			plan.Delete = append(plan.Delete, deleteAction{target: target, value: v})
			plan.TouchedAttrs[target.topLevel()] = struct{}{}
		}
	}

	if len(plan.Set) == 0 && len(plan.Remove) == 0 && len(plan.Add) == 0 && len(plan.Delete) == 0 {
		return updatePlan{}, fmt.Errorf("only SET/REMOVE/ADD/DELETE update expressions are supported")
	}

	if hasPathOverlap(plan) {
		return updatePlan{}, fmt.Errorf("Invalid UpdateExpression: Two document paths overlap with each other; must remove or rewrite one of these paths")
	}

	return plan, nil
}

func parseUpdateClauses(raw string) (map[string]string, error) {
	upper := strings.ToUpper(raw)
	keywords := []string{"SET", "REMOVE", "ADD", "DELETE"}
	type clausePos struct {
		kw  string
		pos int
	}
	positions := []clausePos{}
	depth := 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
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
		for _, kw := range keywords {
			if i+len(kw) > len(raw) {
				continue
			}
			if upper[i:i+len(kw)] != kw {
				continue
			}
			leftBoundary := i == 0 || raw[i-1] == ' '
			rightBoundary := i+len(kw) < len(raw) && raw[i+len(kw)] == ' '
			if leftBoundary && rightBoundary {
				positions = append(positions, clausePos{kw: kw, pos: i})
			}
		}
	}
	if len(positions) == 0 {
		return nil, fmt.Errorf("only SET/REMOVE/ADD/DELETE update expressions are supported")
	}
	out := map[string]string{}
	for i, p := range positions {
		if _, exists := out[p.kw]; exists {
			return nil, fmt.Errorf("Invalid UpdateExpression: Syntax error; token: \"%s\", near: \"%s\"", p.kw, raw[p.pos:])
		}
		start := p.pos + len(p.kw) + 1
		end := len(raw)
		if i+1 < len(positions) {
			end = positions[i+1].pos
		}
		content := strings.TrimSpace(raw[start:end])
		if content == "" {
			return nil, fmt.Errorf("Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"%s \"", p.kw)
		}
		out[p.kw] = content
	}
	return out, nil
}

func parseSetValueExpr(raw string, names map[string]string, values map[string]any) (setValueExpr, error) {
	raw = strings.TrimSpace(raw)
	lower := strings.ToLower(raw)

	if strings.HasPrefix(lower, "if_not_exists(") && strings.HasSuffix(raw, ")") {
		inside := strings.TrimSpace(raw[len("if_not_exists(") : len(raw)-1])
		arg1, arg2, err := parseTwoArgs(inside)
		if err != nil {
			return nil, err
		}
		path, err := parseDocumentPath(arg1, names)
		if err != nil {
			return nil, err
		}
		def, err := parseSetValueExpr(arg2, names, values)
		if err != nil {
			return nil, err
		}
		return ifNotExistsExpr{path: path, defaultVal: def}, nil
	}

	if strings.HasPrefix(lower, "list_append(") && strings.HasSuffix(raw, ")") {
		inside := strings.TrimSpace(raw[len("list_append(") : len(raw)-1])
		arg1, arg2, err := parseTwoArgs(inside)
		if err != nil {
			return nil, err
		}
		left, err := parseSetValueExpr(arg1, names, values)
		if err != nil {
			return nil, err
		}
		right, err := parseSetValueExpr(arg2, names, values)
		if err != nil {
			return nil, err
		}
		return listAppendExpr{left: left, right: right}, nil
	}

	for _, op := range []string{"+", "-"} {
		parts := splitByOperatorTopLevel(raw, op)
		if len(parts) == 2 {
			left, err := parseSetValueExpr(parts[0], names, values)
			if err != nil {
				return nil, err
			}
			right, err := parseSetValueExpr(parts[1], names, values)
			if err != nil {
				return nil, err
			}
			return arithmeticExpr{left: left, right: right, op: op}, nil
		}
	}

	if strings.HasPrefix(raw, ":") {
		v, err := lookupExpressionValue(raw, values)
		if err != nil {
			return nil, err
		}
		return valueLiteralExpr{value: v}, nil
	}

	path, err := parseDocumentPath(raw, names)
	if err != nil {
		return nil, err
	}
	return pathValueExpr{path: path}, nil
}

func lookupExpressionValue(token string, values map[string]any) (any, error) {
	v, ok := values[strings.TrimSpace(token)]
	if !ok {
		return nil, fmt.Errorf("Invalid UpdateExpression: An expression attribute value used in expression is not defined; attribute value: %s", strings.TrimSpace(token))
	}
	return v, nil
}

func applyUpdatePlan(current map[string]any, plan updatePlan) (map[string]any, map[string]any, error) {
	next := cloneItem(current)
	changed := map[string]any{}

	for _, act := range plan.Set {
		v, err := act.value.resolve(next)
		if err != nil {
			return nil, nil, err
		}
		if err := setAtPath(next, act.target, v); err != nil {
			return nil, nil, err
		}
		changed[act.target.topLevel()] = next[act.target.topLevel()]
	}

	for _, path := range plan.Remove {
		if err := removeAtPath(next, path); err != nil {
			return nil, nil, err
		}
		if top := path.topLevel(); top != "" {
			if v, ok := next[top]; ok {
				changed[top] = v
			} else {
				changed[top] = nil
			}
		}
	}

	for _, act := range plan.Add {
		existing, _, err := getAtPath(next, act.target)
		if err != nil {
			return nil, nil, err
		}
		result, err := applyAddValue(existing, act.value)
		if err != nil {
			return nil, nil, err
		}
		if err := setAtPath(next, act.target, result); err != nil {
			return nil, nil, err
		}
		changed[act.target.topLevel()] = next[act.target.topLevel()]
	}

	for _, act := range plan.Delete {
		existing, ok, err := getAtPath(next, act.target)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			continue
		}
		result, err := applyDeleteFromSet(existing, act.value)
		if err != nil {
			return nil, nil, err
		}
		if result == nil {
			if err := removeAtPath(next, act.target); err != nil {
				return nil, nil, err
			}
		} else {
			if err := setAtPath(next, act.target, result); err != nil {
				return nil, nil, err
			}
		}
		if top := act.target.topLevel(); top != "" {
			if v, ok := next[top]; ok {
				changed[top] = v
			} else {
				changed[top] = nil
			}
		}
	}

	return next, changed, nil
}

func parseDocumentPath(raw string, names map[string]string) (documentPath, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return documentPath{}, fmt.Errorf("empty attribute name")
	}
	parts := strings.Split(raw, ".")
	segments := make([]pathSegment, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return documentPath{}, fmt.Errorf("empty attribute name")
		}
		base := part
		if idx := strings.Index(part, "["); idx >= 0 {
			base = part[:idx]
		}
		base = strings.TrimSpace(base)
		resolved, err := resolveNameStrict(base, names)
		if err != nil {
			return documentPath{}, err
		}
		if resolved == "" {
			return documentPath{}, fmt.Errorf("empty attribute name")
		}
		seg := pathSegment{name: resolved}
		rest := strings.TrimSpace(part[len(base):])
		for rest != "" {
			if !strings.HasPrefix(rest, "[") {
				return documentPath{}, fmt.Errorf("invalid document path")
			}
			end := strings.Index(rest, "]")
			if end <= 1 {
				return documentPath{}, fmt.Errorf("invalid document path")
			}
			idxVal, err := strconv.Atoi(strings.TrimSpace(rest[1:end]))
			if err != nil || idxVal < 0 {
				return documentPath{}, fmt.Errorf("invalid document path")
			}
			seg.indexes = append(seg.indexes, idxVal)
			rest = strings.TrimSpace(rest[end+1:])
		}
		segments = append(segments, seg)
	}
	return documentPath{segments: segments}, nil
}

func getAtPath(item map[string]any, path documentPath) (any, bool, error) {
	if len(path.segments) == 0 {
		return nil, false, fmt.Errorf("invalid document path")
	}
	current, ok := item[path.segments[0].name]
	if !ok {
		return nil, false, nil
	}
	for _, idx := range path.segments[0].indexes {
		list, ok := current.(map[string]any)["L"].([]any)
		if !ok || idx < 0 || idx >= len(list) {
			return nil, false, nil
		}
		current = list[idx]
	}
	for i := 1; i < len(path.segments); i++ {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false, nil
		}
		doc, ok := m["M"].(map[string]any)
		if !ok {
			return nil, false, nil
		}
		seg := path.segments[i]
		next, ok := doc[seg.name]
		if !ok {
			return nil, false, nil
		}
		current = next
		for _, idx := range seg.indexes {
			list, ok := current.(map[string]any)["L"].([]any)
			if !ok || idx < 0 || idx >= len(list) {
				return nil, false, nil
			}
			current = list[idx]
		}
	}
	return current, true, nil
}

func setAtPath(item map[string]any, path documentPath, value any) error {
	if len(path.segments) == 0 {
		return fmt.Errorf("invalid document path")
	}
	if len(path.segments) == 1 && len(path.segments[0].indexes) == 0 {
		item[path.segments[0].name] = value
		return nil
	}

	seg := path.segments[0]
	current, ok := item[seg.name]
	if !ok {
		current = map[string]any{"M": map[string]any{}}
		if len(seg.indexes) > 0 {
			current = map[string]any{"L": []any{}}
		}
		item[seg.name] = current
	}
	var err error
	current, err = ensureIndexPath(current, seg.indexes, true)
	if err != nil {
		return err
	}
	if len(path.segments) == 1 {
		if len(seg.indexes) == 0 {
			item[seg.name] = value
			return nil
		}
		base := item[seg.name]
		updated, err := setListLeaf(base, seg.indexes, value)
		if err != nil {
			return err
		}
		item[seg.name] = updated
		return nil
	}

	parent := current
	for i := 1; i < len(path.segments)-1; i++ {
		m, ok := parent.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid document path")
		}
		doc, ok := m["M"].(map[string]any)
		if !ok {
			return fmt.Errorf("invalid document path")
		}
		nextSeg := path.segments[i]
		next, ok := doc[nextSeg.name]
		if !ok {
			next = map[string]any{"M": map[string]any{}}
			if len(nextSeg.indexes) > 0 {
				next = map[string]any{"L": []any{}}
			}
			doc[nextSeg.name] = next
		}
		next, err = ensureIndexPath(next, nextSeg.indexes, true)
		if err != nil {
			return err
		}
		parent = next
	}

	last := path.segments[len(path.segments)-1]
	m, ok := parent.(map[string]any)
	if !ok {
		return fmt.Errorf("invalid document path")
	}
	doc, ok := m["M"].(map[string]any)
	if !ok {
		return fmt.Errorf("invalid document path")
	}
	if len(last.indexes) == 0 {
		doc[last.name] = value
		return nil
	}
	base, ok := doc[last.name]
	if !ok {
		base = map[string]any{"L": []any{}}
	}
	updated, err := setListLeaf(base, last.indexes, value)
	if err != nil {
		return err
	}
	doc[last.name] = updated
	return nil
}

func ensureIndexPath(base any, indexes []int, create bool) (any, error) {
	current := base
	for _, idx := range indexes {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid document path")
		}
		list, ok := m["L"].([]any)
		if !ok {
			return nil, fmt.Errorf("invalid document path")
		}
		if idx < 0 {
			return nil, fmt.Errorf("invalid document path")
		}
		if idx >= len(list) {
			if !create {
				return nil, fmt.Errorf("invalid document path")
			}
			for len(list) <= idx {
				list = append(list, map[string]any{"M": map[string]any{}})
			}
			m["L"] = list
		}
		current = list[idx]
	}
	return current, nil
}

func setListLeaf(base any, indexes []int, value any) (any, error) {
	if len(indexes) == 0 {
		return value, nil
	}
	m, ok := base.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid document path")
	}
	list, ok := m["L"].([]any)
	if !ok {
		return nil, fmt.Errorf("invalid document path")
	}
	idx := indexes[0]
	if idx < 0 {
		return nil, fmt.Errorf("invalid document path")
	}
	if idx > len(list) {
		return nil, fmt.Errorf("invalid document path")
	}
	if idx == len(list) {
		if len(indexes) > 1 {
			list = append(list, map[string]any{"L": []any{}})
		} else {
			list = append(list, value)
			m["L"] = list
			return m, nil
		}
	}
	next, err := setListLeaf(list[idx], indexes[1:], value)
	if err != nil {
		return nil, err
	}
	list[idx] = next
	m["L"] = list
	return m, nil
}

func removeAtPath(item map[string]any, path documentPath) error {
	if len(path.segments) == 0 {
		return fmt.Errorf("invalid document path")
	}
	if len(path.segments) == 1 && len(path.segments[0].indexes) == 0 {
		delete(item, path.segments[0].name)
		return nil
	}

	parent, key, idxs, ok, err := resolveParentContainer(item, path)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if len(idxs) == 0 {
		doc := parent[key].(map[string]any)["M"].(map[string]any)
		delete(doc, path.segments[len(path.segments)-1].name)
		return nil
	}
	base := parent[key]
	updated, err := removeFromListPath(base, idxs)
	if err != nil {
		return err
	}
	parent[key] = updated
	return nil
}

func resolveParentContainer(item map[string]any, path documentPath) (map[string]any, string, []int, bool, error) {
	if len(path.segments) == 0 {
		return nil, "", nil, false, fmt.Errorf("invalid document path")
	}
	if len(path.segments) == 1 {
		return item, path.segments[0].name, path.segments[0].indexes, true, nil
	}
	first := path.segments[0]
	current, ok := item[first.name]
	if !ok {
		return nil, "", nil, false, nil
	}
	for _, idx := range first.indexes {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, "", nil, false, nil
		}
		list, ok := m["L"].([]any)
		if !ok || idx < 0 || idx >= len(list) {
			return nil, "", nil, false, nil
		}
		current = list[idx]
	}
	for i := 1; i < len(path.segments)-1; i++ {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, "", nil, false, nil
		}
		doc, ok := m["M"].(map[string]any)
		if !ok {
			return nil, "", nil, false, nil
		}
		seg := path.segments[i]
		next, ok := doc[seg.name]
		if !ok {
			return nil, "", nil, false, nil
		}
		current = next
		for _, idx := range seg.indexes {
			m, ok := current.(map[string]any)
			if !ok {
				return nil, "", nil, false, nil
			}
			list, ok := m["L"].([]any)
			if !ok || idx < 0 || idx >= len(list) {
				return nil, "", nil, false, nil
			}
			current = list[idx]
		}
	}
	parentMap, ok := current.(map[string]any)
	if !ok {
		return nil, "", nil, false, nil
	}
	doc, ok := parentMap["M"].(map[string]any)
	if !ok {
		return nil, "", nil, false, nil
	}
	last := path.segments[len(path.segments)-1]
	if _, exists := doc[last.name]; !exists {
		return nil, "", nil, false, nil
	}
	return doc, last.name, last.indexes, true, nil
}

func removeFromListPath(base any, indexes []int) (any, error) {
	m, ok := base.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid document path")
	}
	list, ok := m["L"].([]any)
	if !ok {
		return nil, fmt.Errorf("invalid document path")
	}
	idx := indexes[0]
	if idx < 0 || idx >= len(list) {
		return nil, nil
	}
	if len(indexes) == 1 {
		list = append(list[:idx], list[idx+1:]...)
		m["L"] = list
		return m, nil
	}
	updated, err := removeFromListPath(list[idx], indexes[1:])
	if err != nil {
		return nil, err
	}
	if updated != nil {
		list[idx] = updated
		m["L"] = list
	}
	return m, nil
}

func applyAddValue(existing any, delta any) (any, error) {
	if dn, ok := exprNumber(delta); ok {
		base := 0.0
		if existing != nil {
			en, ok := exprNumber(existing)
			if !ok {
				return nil, fmt.Errorf("ADD action supports only Number and Set data types")
			}
			base = en
		}
		return map[string]any{"N": trimTrailingZeros(base + dn)}, nil
	}

	dType, dVals, err := decodeSetAttribute(delta)
	if err != nil {
		return nil, fmt.Errorf("ADD action supports only Number and Set data types")
	}
	if existing == nil {
		copyVals := make([]any, len(dVals))
		for i, v := range dVals {
			copyVals[i] = v
		}
		return map[string]any{dType: copyVals}, nil
	}
	eType, eVals, err := decodeSetAttribute(existing)
	if err != nil {
		return nil, fmt.Errorf("ADD action supports only Number and Set data types")
	}
	if eType != dType {
		return nil, fmt.Errorf("ADD action supports only Number and Set data types")
	}
	seen := map[string]struct{}{}
	out := make([]any, 0, len(eVals)+len(dVals))
	for _, v := range eVals {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	for _, v := range dVals {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return map[string]any{eType: out}, nil
}

func applyDeleteFromSet(current any, subset any) (any, error) {
	setType, currentValues, err := decodeSetAttribute(current)
	if err != nil {
		return nil, err
	}
	deleteType, deleteValues, err := decodeSetAttribute(subset)
	if err != nil {
		return nil, err
	}
	if setType != deleteType {
		return nil, fmt.Errorf("DELETE action requires both operands to be the same set type")
	}
	if len(deleteValues) == 0 {
		return nil, fmt.Errorf("DELETE action requires a non-empty set value")
	}

	remove := make(map[string]struct{}, len(deleteValues))
	for _, v := range deleteValues {
		remove[v] = struct{}{}
	}
	remaining := make([]any, 0, len(currentValues))
	for _, v := range currentValues {
		if _, ok := remove[v]; ok {
			continue
		}
		remaining = append(remaining, v)
	}
	if len(remaining) == 0 {
		return nil, nil
	}
	return map[string]any{setType: remaining}, nil
}

func decodeSetAttribute(v any) (string, []string, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return "", nil, fmt.Errorf("DELETE action supports only SS/NS/BS set attributes")
	}
	for _, setType := range []string{"SS", "NS", "BS"} {
		raw, ok := m[setType]
		if !ok {
			continue
		}
		listAny, ok := raw.([]any)
		if ok {
			out := make([]string, 0, len(listAny))
			for _, item := range listAny {
				s, ok := item.(string)
				if !ok {
					return "", nil, fmt.Errorf("DELETE action supports only SS/NS/BS set attributes")
				}
				out = append(out, s)
			}
			return setType, out, nil
		}
		listStr, ok := raw.([]string)
		if ok {
			out := make([]string, len(listStr))
			copy(out, listStr)
			return setType, out, nil
		}
		return "", nil, fmt.Errorf("DELETE action supports only SS/NS/BS set attributes")
	}
	return "", nil, fmt.Errorf("DELETE action supports only SS/NS/BS set attributes")
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
