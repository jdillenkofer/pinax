package expr

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type tokenKind int

const (
	tokenEOF tokenKind = iota
	tokenIdent
	tokenName
	tokenValue
	tokenLParen
	tokenRParen
	tokenComma
	tokenDot
	tokenLBracket
	tokenRBracket
	tokenEq
	tokenNe
	tokenLt
	tokenLe
	tokenGt
	tokenGe
	tokenNumber
)

type token struct {
	kind tokenKind
	text string
}

type KeyCondition struct {
	Partition KeyConditionTerm
	Sort      *KeyConditionTerm
}

type KeyConditionTerm struct {
	Attribute string
	Operator  string
	Value1    string
	Value2    string
}

func ParseKeyCondition(condition string) (KeyCondition, error) {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return KeyCondition{}, fmt.Errorf("KeyConditionExpression is required")
	}
	tokens, err := lexExpression(condition)
	if err != nil {
		return KeyCondition{}, err
	}
	p := exprParser{tokens: tokens}
	partition, err := p.parsePartitionKeyCondition()
	if err != nil {
		return KeyCondition{}, err
	}
	out := KeyCondition{Partition: partition}
	if isKeyword(p.current(), "AND") {
		p.next()
		sort, err := p.parseSortKeyCondition()
		if err != nil {
			return KeyCondition{}, err
		}
		out.Sort = &sort
	}
	if p.current().kind != tokenEOF {
		return KeyCondition{}, fmt.Errorf("unsupported sort key condition")
	}
	return out, nil
}

func evaluateParsed(condition string, item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	tokens, err := lexExpression(condition)
	if err != nil {
		return false, err
	}
	p := exprParser{tokens: tokens}
	node, err := p.parseBooleanExpression()
	if err != nil {
		return false, err
	}
	if p.current().kind != tokenEOF {
		return false, fmt.Errorf("unsupported condition expression")
	}
	ctx := evalContext{item: item, names: names, values: values}
	return node.eval(ctx)
}

func lexExpression(input string) ([]token, error) {
	tokens := make([]token, 0, len(input)/3)
	for i := 0; i < len(input); {
		r := rune(input[i])
		if unicode.IsSpace(r) {
			i++
			continue
		}

		switch {
		case i+1 < len(input) && input[i:i+2] == "<>":
			tokens = append(tokens, token{kind: tokenNe, text: "<>"})
			i += 2
		case i+1 < len(input) && input[i:i+2] == "<=":
			tokens = append(tokens, token{kind: tokenLe, text: "<="})
			i += 2
		case i+1 < len(input) && input[i:i+2] == ">=":
			tokens = append(tokens, token{kind: tokenGe, text: ">="})
			i += 2
		case input[i] == '=':
			tokens = append(tokens, token{kind: tokenEq, text: "="})
			i++
		case input[i] == '<':
			tokens = append(tokens, token{kind: tokenLt, text: "<"})
			i++
		case input[i] == '>':
			tokens = append(tokens, token{kind: tokenGt, text: ">"})
			i++
		case input[i] == '(':
			tokens = append(tokens, token{kind: tokenLParen, text: "("})
			i++
		case input[i] == ')':
			tokens = append(tokens, token{kind: tokenRParen, text: ")"})
			i++
		case input[i] == ',':
			tokens = append(tokens, token{kind: tokenComma, text: ","})
			i++
		case input[i] == '.':
			tokens = append(tokens, token{kind: tokenDot, text: "."})
			i++
		case input[i] == '[':
			tokens = append(tokens, token{kind: tokenLBracket, text: "["})
			i++
		case input[i] == ']':
			tokens = append(tokens, token{kind: tokenRBracket, text: "]"})
			i++
		case input[i] == '#':
			j := i + 1
			for j < len(input) && isIdentifierPart(rune(input[j])) {
				j++
			}
			if j == i+1 {
				return nil, fmt.Errorf("unsupported condition expression")
			}
			tokens = append(tokens, token{kind: tokenName, text: input[i:j]})
			i = j
		case input[i] == ':':
			j := i + 1
			for j < len(input) && isIdentifierPart(rune(input[j])) {
				j++
			}
			if j == i+1 {
				return nil, fmt.Errorf("unsupported condition expression")
			}
			tokens = append(tokens, token{kind: tokenValue, text: input[i:j]})
			i = j
		case isIdentifierStart(r):
			j := i + 1
			for j < len(input) && isIdentifierPart(rune(input[j])) {
				j++
			}
			tokens = append(tokens, token{kind: tokenIdent, text: input[i:j]})
			i = j
		case unicode.IsDigit(r):
			j := i + 1
			for j < len(input) && unicode.IsDigit(rune(input[j])) {
				j++
			}
			tokens = append(tokens, token{kind: tokenNumber, text: input[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unsupported condition expression")
		}
	}
	tokens = append(tokens, token{kind: tokenEOF})
	return tokens, nil
}

func isIdentifierStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isIdentifierPart(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

type exprParser struct {
	tokens []token
	pos    int
}

func (p *exprParser) current() token {
	if p.pos >= len(p.tokens) {
		return token{kind: tokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *exprParser) next() token {
	t := p.current()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return t
}

func (p *exprParser) parseBooleanExpression() (boolExpr, error) {
	return p.parseOr()
}

func (p *exprParser) parsePartitionKeyCondition() (KeyConditionTerm, error) {
	left, err := p.parsePathOperand()
	if err != nil {
		return KeyConditionTerm{}, fmt.Errorf("partition key condition must use '='")
	}
	if p.current().kind != tokenEq {
		return KeyConditionTerm{}, fmt.Errorf("partition key condition must use '='")
	}
	p.next()
	if p.current().kind != tokenValue {
		return KeyConditionTerm{}, fmt.Errorf("invalid key condition segment")
	}
	right := p.next().text
	if strings.TrimSpace(left.path) == "" || strings.TrimSpace(right) == "" {
		return KeyConditionTerm{}, fmt.Errorf("invalid key condition segment")
	}
	return KeyConditionTerm{Attribute: left.path, Operator: "=", Value1: right}, nil
}

func (p *exprParser) parseSortKeyCondition() (KeyConditionTerm, error) {
	if p.current().kind == tokenIdent && strings.EqualFold(p.current().text, "begins_with") {
		p.next()
		if p.current().kind != tokenLParen {
			return KeyConditionTerm{}, fmt.Errorf("invalid begins_with sort key condition")
		}
		p.next()
		left, err := p.parsePathOperand()
		if err != nil {
			return KeyConditionTerm{}, fmt.Errorf("invalid begins_with sort key condition")
		}
		if p.current().kind != tokenComma {
			return KeyConditionTerm{}, fmt.Errorf("invalid begins_with sort key condition")
		}
		p.next()
		if p.current().kind != tokenValue {
			return KeyConditionTerm{}, fmt.Errorf("invalid begins_with sort key condition")
		}
		v := p.next().text
		if p.current().kind != tokenRParen {
			return KeyConditionTerm{}, fmt.Errorf("invalid begins_with sort key condition")
		}
		p.next()
		return KeyConditionTerm{Attribute: left.path, Operator: "begins_with", Value1: v}, nil
	}

	left, err := p.parsePathOperand()
	if err != nil {
		return KeyConditionTerm{}, fmt.Errorf("unsupported sort key condition")
	}

	if isKeyword(p.current(), "BETWEEN") {
		p.next()
		if p.current().kind != tokenValue {
			return KeyConditionTerm{}, fmt.Errorf("invalid BETWEEN sort key condition")
		}
		v1 := p.next().text
		if !isKeyword(p.current(), "AND") {
			return KeyConditionTerm{}, fmt.Errorf("invalid BETWEEN sort key condition")
		}
		p.next()
		if p.current().kind != tokenValue {
			return KeyConditionTerm{}, fmt.Errorf("BETWEEN requires two values")
		}
		v2 := p.next().text
		if strings.TrimSpace(v1) == "" || strings.TrimSpace(v2) == "" {
			return KeyConditionTerm{}, fmt.Errorf("BETWEEN requires two values")
		}
		return KeyConditionTerm{Attribute: left.path, Operator: "BETWEEN", Value1: v1, Value2: v2}, nil
	}

	switch p.current().kind {
	case tokenEq, tokenLt, tokenLe, tokenGt, tokenGe:
		op := p.next().text
		if p.current().kind != tokenValue {
			return KeyConditionTerm{}, fmt.Errorf("unsupported sort key condition")
		}
		v := p.next().text
		return KeyConditionTerm{Attribute: left.path, Operator: op, Value1: v}, nil
	default:
		return KeyConditionTerm{}, fmt.Errorf("unsupported sort key condition")
	}
}

func (p *exprParser) parseOr() (boolExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for isKeyword(p.current(), "OR") {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = orExpr{left: left, right: right}
	}
	return left, nil
}

func (p *exprParser) parseAnd() (boolExpr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for isKeyword(p.current(), "AND") {
		p.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = andExpr{left: left, right: right}
	}
	return left, nil
}

func (p *exprParser) parseNot() (boolExpr, error) {
	if isKeyword(p.current(), "NOT") {
		p.next()
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return notExpr{inner: inner}, nil
	}
	return p.parsePrimary()
}

func (p *exprParser) parsePrimary() (boolExpr, error) {
	if p.current().kind == tokenLParen {
		p.next()
		node, err := p.parseBooleanExpression()
		if err != nil {
			return nil, err
		}
		if p.current().kind != tokenRParen {
			return nil, fmt.Errorf("unsupported condition expression")
		}
		p.next()
		return node, nil
	}
	return p.parsePredicate()
}

func (p *exprParser) parsePredicate() (boolExpr, error) {
	if p.current().kind == tokenIdent && p.peek().kind == tokenLParen {
		fn := strings.ToLower(p.current().text)
		switch fn {
		case "attribute_exists", "attribute_not_exists", "begins_with", "contains", "attribute_type":
			return p.parseFunctionPredicate()
		}
	}

	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	if isKeyword(p.current(), "BETWEEN") {
		p.next()
		lower, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if !isKeyword(p.current(), "AND") {
			return nil, fmt.Errorf("unsupported condition expression")
		}
		p.next()
		upper, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return betweenExpr{left: left, lower: lower, upper: upper}, nil
	}

	if isKeyword(p.current(), "IN") {
		p.next()
		if p.current().kind != tokenLParen {
			return nil, fmt.Errorf("IN requires parenthesized values")
		}
		p.next()
		list := []operandExpr{}
		for {
			op, err := p.parseOperand()
			if err != nil {
				return nil, err
			}
			list = append(list, op)
			if p.current().kind == tokenComma {
				p.next()
				continue
			}
			break
		}
		if p.current().kind != tokenRParen {
			return nil, fmt.Errorf("IN requires parenthesized values")
		}
		p.next()
		return inExpr{left: left, values: list}, nil
	}

	switch p.current().kind {
	case tokenEq, tokenNe, tokenLt, tokenLe, tokenGt, tokenGe:
		op := p.next().text
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return compareExpr{left: left, right: right, op: op}, nil
	default:
		return nil, fmt.Errorf("unsupported condition expression")
	}
}

func (p *exprParser) parseFunctionPredicate() (boolExpr, error) {
	fnTok := p.next()
	fn := strings.ToLower(fnTok.text)
	if p.current().kind != tokenLParen {
		return nil, fmt.Errorf("unsupported condition expression")
	}
	p.next()

	switch fn {
	case "attribute_exists", "attribute_not_exists":
		path, err := p.parsePathOperand()
		if err != nil {
			return nil, err
		}
		if p.current().kind != tokenRParen {
			return nil, fmt.Errorf("unsupported condition expression")
		}
		p.next()
		if fn == "attribute_exists" {
			return attributeExistsExpr{path: path.path}, nil
		}
		return attributeNotExistsExpr{path: path.path}, nil
	case "begins_with", "contains", "attribute_type":
		path, err := p.parsePathOperand()
		if err != nil {
			return nil, err
		}
		if p.current().kind != tokenComma {
			return nil, fmt.Errorf("expected two arguments")
		}
		p.next()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if p.current().kind != tokenRParen {
			return nil, fmt.Errorf("expected two arguments")
		}
		p.next()
		switch fn {
		case "begins_with":
			return beginsWithExpr{leftPath: path.path, right: right}, nil
		case "contains":
			return containsExpr{leftPath: path.path, right: right}, nil
		default:
			return attributeTypeExpr{leftPath: path.path, right: right}, nil
		}
	default:
		return nil, fmt.Errorf("unsupported condition expression")
	}
}

func (p *exprParser) parseOperand() (operandExpr, error) {
	tok := p.current()
	switch tok.kind {
	case tokenValue:
		p.next()
		return valueOperand{token: tok.text}, nil
	case tokenIdent:
		if strings.EqualFold(tok.text, "size") && p.peek().kind == tokenLParen {
			p.next()
			p.next()
			inner, err := p.parsePathOperand()
			if err != nil {
				return nil, err
			}
			if p.current().kind != tokenRParen {
				return nil, fmt.Errorf("unsupported condition expression")
			}
			p.next()
			return sizeOperand{inner: inner}, nil
		}
		return p.parsePathOperand()
	case tokenName:
		return p.parsePathOperand()
	default:
		return nil, fmt.Errorf("unsupported condition expression")
	}
}

func (p *exprParser) parsePathOperand() (pathOperand, error) {
	segTok := p.current()
	if segTok.kind != tokenIdent && segTok.kind != tokenName {
		return pathOperand{}, fmt.Errorf("unsupported condition expression")
	}
	p.next()

	segment, err := p.parseSegment(segTok.text)
	if err != nil {
		return pathOperand{}, err
	}
	segments := []string{segment}

	for p.current().kind == tokenDot {
		p.next()
		nextTok := p.current()
		if nextTok.kind != tokenIdent && nextTok.kind != tokenName {
			return pathOperand{}, fmt.Errorf("unsupported condition expression")
		}
		p.next()
		nextSegment, err := p.parseSegment(nextTok.text)
		if err != nil {
			return pathOperand{}, err
		}
		segments = append(segments, nextSegment)
	}

	return pathOperand{path: strings.Join(segments, ".")}, nil
}

func (p *exprParser) parseSegment(base string) (string, error) {
	var b strings.Builder
	b.WriteString(base)
	for p.current().kind == tokenLBracket {
		p.next()
		if p.current().kind != tokenNumber {
			return "", fmt.Errorf("unsupported condition expression")
		}
		indexText := p.current().text
		if _, err := strconv.Atoi(indexText); err != nil {
			return "", fmt.Errorf("unsupported condition expression")
		}
		b.WriteString("[")
		b.WriteString(indexText)
		b.WriteString("]")
		p.next()
		if p.current().kind != tokenRBracket {
			return "", fmt.Errorf("unsupported condition expression")
		}
		p.next()
	}
	return b.String(), nil
}

func (p *exprParser) peek() token {
	if p.pos+1 >= len(p.tokens) {
		return token{kind: tokenEOF}
	}
	return p.tokens[p.pos+1]
}

func isKeyword(tok token, keyword string) bool {
	return tok.kind == tokenIdent && strings.EqualFold(tok.text, keyword)
}

type evalContext struct {
	item   map[string]any
	names  map[string]string
	values map[string]any
}

type boolExpr interface {
	eval(ctx evalContext) (bool, error)
}

type operandExpr interface {
	resolve(ctx evalContext) (any, bool, error)
}

type orExpr struct {
	left  boolExpr
	right boolExpr
}

func (e orExpr) eval(ctx evalContext) (bool, error) {
	l, err := e.left.eval(ctx)
	if err != nil {
		return false, err
	}
	if l {
		return true, nil
	}
	return e.right.eval(ctx)
}

type andExpr struct {
	left  boolExpr
	right boolExpr
}

func (e andExpr) eval(ctx evalContext) (bool, error) {
	l, err := e.left.eval(ctx)
	if err != nil {
		return false, err
	}
	if !l {
		return false, nil
	}
	return e.right.eval(ctx)
}

type notExpr struct {
	inner boolExpr
}

func (e notExpr) eval(ctx evalContext) (bool, error) {
	inner, err := e.inner.eval(ctx)
	if err != nil {
		return false, err
	}
	return !inner, nil
}

type compareExpr struct {
	left  operandExpr
	right operandExpr
	op    string
}

func (e compareExpr) eval(ctx evalContext) (bool, error) {
	left, leftOK, err := e.left.resolve(ctx)
	if err != nil {
		return false, err
	}
	if !leftOK {
		return false, nil
	}
	right, rightOK, err := e.right.resolve(ctx)
	if err != nil {
		return false, err
	}
	if !rightOK {
		return false, nil
	}
	if e.op == "<>" {
		eq, err := compare(left, right, "=")
		if err != nil {
			return false, err
		}
		return !eq, nil
	}
	return compare(left, right, e.op)
}

type betweenExpr struct {
	left  operandExpr
	lower operandExpr
	upper operandExpr
}

func (e betweenExpr) eval(ctx evalContext) (bool, error) {
	left, leftOK, err := e.left.resolve(ctx)
	if err != nil {
		return false, err
	}
	if !leftOK {
		return false, nil
	}
	lower, lowerOK, err := e.lower.resolve(ctx)
	if err != nil {
		return false, err
	}
	if !lowerOK {
		return false, nil
	}
	upper, upperOK, err := e.upper.resolve(ctx)
	if err != nil {
		return false, err
	}
	if !upperOK {
		return false, nil
	}
	okLower, err := compare(left, lower, ">=")
	if err != nil {
		return false, err
	}
	if !okLower {
		return false, nil
	}
	return compare(left, upper, "<=")
}

type inExpr struct {
	left   operandExpr
	values []operandExpr
}

func (e inExpr) eval(ctx evalContext) (bool, error) {
	left, leftOK, err := e.left.resolve(ctx)
	if err != nil {
		return false, err
	}
	if !leftOK {
		return false, nil
	}
	for _, candidateExpr := range e.values {
		candidate, ok, err := candidateExpr.resolve(ctx)
		if err != nil {
			return false, err
		}
		if !ok {
			continue
		}
		match, err := compare(left, candidate, "=")
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

type attributeExistsExpr struct {
	path string
}

func (e attributeExistsExpr) eval(ctx evalContext) (bool, error) {
	resolved, err := resolveAttr(e.path, ctx.names)
	if err != nil {
		return false, err
	}
	_, ok := getPathValue(ctx.item, resolved)
	return ok, nil
}

type attributeNotExistsExpr struct {
	path string
}

func (e attributeNotExistsExpr) eval(ctx evalContext) (bool, error) {
	resolved, err := resolveAttr(e.path, ctx.names)
	if err != nil {
		return false, err
	}
	_, ok := getPathValue(ctx.item, resolved)
	return !ok, nil
}

type beginsWithExpr struct {
	leftPath string
	right    operandExpr
}

func (e beginsWithExpr) eval(ctx evalContext) (bool, error) {
	resolved, err := resolveAttr(e.leftPath, ctx.names)
	if err != nil {
		return false, err
	}
	left, ok := getPathValue(ctx.item, resolved)
	if !ok {
		return false, nil
	}
	right, rightOK, err := e.right.resolve(ctx)
	if err != nil {
		return false, err
	}
	if !rightOK {
		return false, nil
	}
	leftS, leftString := attrString(left)
	rightS, rightString := attrString(right)
	if !leftString || !rightString {
		return false, fmt.Errorf("begins_with requires string operands")
	}
	return strings.HasPrefix(leftS, rightS), nil
}

type containsExpr struct {
	leftPath string
	right    operandExpr
}

func (e containsExpr) eval(ctx evalContext) (bool, error) {
	resolved, err := resolveAttr(e.leftPath, ctx.names)
	if err != nil {
		return false, err
	}
	left, ok := getPathValue(ctx.item, resolved)
	if !ok {
		return false, nil
	}
	right, rightOK, err := e.right.resolve(ctx)
	if err != nil {
		return false, err
	}
	if !rightOK {
		return false, nil
	}

	leftS, leftString := attrString(left)
	rightS, rightString := attrString(right)
	if leftString && rightString {
		return strings.Contains(leftS, rightS), nil
	}

	leftMap, mapOK := left.(map[string]any)
	if !mapOK {
		return false, fmt.Errorf("contains supports string, list, and set operands")
	}
	if list, ok := leftMap["L"].([]any); ok {
		for _, elem := range list {
			match, err := compare(elem, right, "=")
			if err != nil {
				continue
			}
			if match {
				return true, nil
			}
		}
		return false, nil
	}
	if ss, ok := leftMap["SS"].([]any); ok {
		rightS, ok := attrString(right)
		if !ok {
			return false, fmt.Errorf("contains with SS requires string operand")
		}
		for _, v := range ss {
			if s, ok := v.(string); ok && s == rightS {
				return true, nil
			}
		}
		return false, nil
	}
	if ns, ok := leftMap["NS"].([]any); ok {
		rightN, ok := attrNumber(right)
		if !ok {
			return false, fmt.Errorf("contains with NS requires number operand")
		}
		for _, v := range ns {
			s, ok := v.(string)
			if !ok {
				continue
			}
			n, err := strconv.ParseFloat(s, 64)
			if err == nil && n == rightN {
				return true, nil
			}
		}
		return false, nil
	}
	if bs, ok := leftMap["BS"].([]any); ok {
		rightB, ok := attrBinary(right)
		if !ok {
			return false, fmt.Errorf("contains with BS requires binary operand")
		}
		for _, v := range bs {
			s, ok := v.(string)
			if ok && s == rightB {
				return true, nil
			}
		}
		return false, nil
	}

	return false, fmt.Errorf("contains supports string, list, and set operands")
}

type attributeTypeExpr struct {
	leftPath string
	right    operandExpr
}

func (e attributeTypeExpr) eval(ctx evalContext) (bool, error) {
	resolved, err := resolveAttr(e.leftPath, ctx.names)
	if err != nil {
		return false, err
	}
	left, ok := getPathValue(ctx.item, resolved)
	if !ok {
		return false, nil
	}
	right, rightOK, err := e.right.resolve(ctx)
	if err != nil {
		return false, err
	}
	if !rightOK {
		return false, nil
	}
	expected, ok := attrString(right)
	if !ok {
		return false, fmt.Errorf("attribute_type expects string type code")
	}
	return hasAttrType(left, expected), nil
}

type pathOperand struct {
	path string
}

func (o pathOperand) resolve(ctx evalContext) (any, bool, error) {
	resolved, err := resolveAttr(o.path, ctx.names)
	if err != nil {
		return nil, false, err
	}
	v, ok := getPathValue(ctx.item, resolved)
	return v, ok, nil
}

type valueOperand struct {
	token string
}

func (o valueOperand) resolve(ctx evalContext) (any, bool, error) {
	v, ok := ctx.values[strings.TrimSpace(o.token)]
	if !ok {
		return nil, false, fmt.Errorf("missing expression value %q", strings.TrimSpace(o.token))
	}
	return v, true, nil
}

type sizeOperand struct {
	inner operandExpr
}

func (o sizeOperand) resolve(ctx evalContext) (any, bool, error) {
	v, ok, err := o.inner.resolve(ctx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	size, err := attributeSize(v)
	if err != nil {
		return nil, false, err
	}
	return map[string]any{"N": strconv.Itoa(size)}, true, nil
}

func attributeSize(v any) (int, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return 0, fmt.Errorf("size() requires a document path")
	}
	if s, ok := m["S"].(string); ok {
		return len(s), nil
	}
	if b, ok := m["B"].(string); ok {
		return len(b), nil
	}
	if l, ok := m["L"].([]any); ok {
		return len(l), nil
	}
	if mm, ok := m["M"].(map[string]any); ok {
		return len(mm), nil
	}
	if ss, ok := m["SS"].([]any); ok {
		return len(ss), nil
	}
	if ns, ok := m["NS"].([]any); ok {
		return len(ns), nil
	}
	if bs, ok := m["BS"].([]any); ok {
		return len(bs), nil
	}
	return 0, fmt.Errorf("size() supports only String, Binary, List, Map, and Set attributes")
}
