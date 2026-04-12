package httpapi

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"strings"

	"encoding/json"
)

func decode(body []byte, out any) error {
	if len(strings.TrimSpace(string(body))) == 0 {
		body = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	var trailing struct{}
	if err := dec.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("invalid request body: unexpected trailing content")
	}
	return nil
}

func resolveName(v string, names map[string]string) string {
	v = strings.TrimSpace(v)
	if strings.HasPrefix(v, "#") {
		if out, ok := names[v]; ok {
			return out
		}
	}
	return v
}

func resolveNameStrict(v string, names map[string]string) (string, error) {
	v = strings.TrimSpace(v)
	if strings.HasPrefix(v, "#") {
		if out, ok := names[v]; ok {
			return out, nil
		}
		return "", fmt.Errorf("missing expression name %q", v)
	}
	return v, nil
}

func cloneExpressionValues(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func normalizeQuerySelect(req queryRequest, hasIndex bool, isGSI bool) (string, string, error) {
	projectionExpression, err := normalizeLegacyAttributesProjection(req.AttributesToGet, req.ProjectionExpression)
	if err != nil {
		return "", "", err
	}

	selectMode := strings.ToUpper(strings.TrimSpace(req.Select))
	if selectMode == "" {
		switch {
		case projectionExpression != "":
			selectMode = "SPECIFIC_ATTRIBUTES"
		case hasIndex:
			selectMode = "ALL_PROJECTED_ATTRIBUTES"
		default:
			selectMode = "ALL_ATTRIBUTES"
		}
	}

	switch selectMode {
	case "ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "COUNT":
	default:
		return "", "", fmt.Errorf("unsupported Query Select value %q", req.Select)
	}

	if projectionExpression != "" && selectMode != "SPECIFIC_ATTRIBUTES" {
		return "", "", fmt.Errorf("Select can only be SPECIFIC_ATTRIBUTES when ProjectionExpression or AttributesToGet is set")
	}
	if selectMode == "SPECIFIC_ATTRIBUTES" && projectionExpression == "" {
		return "", "", fmt.Errorf("ProjectionExpression or AttributesToGet is required when Select is SPECIFIC_ATTRIBUTES")
	}
	if selectMode == "ALL_PROJECTED_ATTRIBUTES" && !hasIndex {
		return "", "", fmt.Errorf("ALL_PROJECTED_ATTRIBUTES is only valid when querying an index")
	}
	if selectMode == "ALL_ATTRIBUTES" && isGSI {
		return "", "", fmt.Errorf("ALL_ATTRIBUTES is not supported when querying a global secondary index")
	}

	return selectMode, projectionExpression, nil
}

func normalizeLegacyAttributesProjection(attributesToGet []string, projectionExpression string) (string, error) {
	projectionExpression = strings.TrimSpace(projectionExpression)
	if len(attributesToGet) == 0 {
		return projectionExpression, nil
	}
	if projectionExpression != "" {
		return "", fmt.Errorf("AttributesToGet and ProjectionExpression cannot both be set")
	}
	attrs := make([]string, 0, len(attributesToGet))
	seen := map[string]struct{}{}
	for _, attr := range attributesToGet {
		attr = strings.TrimSpace(attr)
		if attr == "" {
			continue
		}
		if _, ok := seen[attr]; ok {
			continue
		}
		seen[attr] = struct{}{}
		attrs = append(attrs, attr)
	}
	return strings.Join(attrs, ", "), nil
}

func parseLegacyQueryKeyConditions(
	keyConditions map[string]struct {
		AttributeValueList []any  `json:"AttributeValueList"`
		ComparisonOperator string `json:"ComparisonOperator"`
	},
	targetHashKey string,
	targetRangeKey string,
	expressionValues map[string]any,
) (keyExprToken, *sortKeyCondition, map[string]any, error) {
	if len(keyConditions) == 0 {
		return keyExprToken{}, nil, expressionValues, fmt.Errorf("KeyConditionExpression is required")
	}

	for attr := range keyConditions {
		if attr != targetHashKey && attr != targetRangeKey {
			return keyExprToken{}, nil, expressionValues, fmt.Errorf("legacy KeyConditions may only include HASH and RANGE key attributes")
		}
	}

	hashCond, ok := keyConditions[targetHashKey]
	if !ok {
		return keyExprToken{}, nil, expressionValues, fmt.Errorf("partition key condition must target HASH key")
	}
	if strings.ToUpper(strings.TrimSpace(hashCond.ComparisonOperator)) != "EQ" {
		return keyExprToken{}, nil, expressionValues, fmt.Errorf("partition key condition must use '='")
	}
	if len(hashCond.AttributeValueList) != 1 {
		return keyExprToken{}, nil, expressionValues, fmt.Errorf("partition key condition requires exactly one value")
	}

	pkToken := nextLegacyToken(expressionValues, ":__legacy_pk")
	expressionValues[pkToken] = hashCond.AttributeValueList[0]
	pk := keyExprToken{attr: targetHashKey, value: pkToken}

	if targetRangeKey == "" {
		if _, exists := keyConditions[targetRangeKey]; exists {
			return keyExprToken{}, nil, expressionValues, fmt.Errorf("sort key condition is not supported for tables without a RANGE key")
		}
		return pk, nil, expressionValues, nil
	}

	rangeCond, ok := keyConditions[targetRangeKey]
	if !ok {
		return pk, nil, expressionValues, nil
	}

	op := strings.ToUpper(strings.TrimSpace(rangeCond.ComparisonOperator))
	sk := &sortKeyCondition{attr: targetRangeKey}
	valueToken1 := nextLegacyToken(expressionValues, ":__legacy_sk")

	switch op {
	case "EQ", "LT", "LE", "GT", "GE", "BEGINS_WITH":
		if len(rangeCond.AttributeValueList) != 1 {
			return keyExprToken{}, nil, expressionValues, fmt.Errorf("sort key condition %s requires exactly one value", op)
		}
		expressionValues[valueToken1] = rangeCond.AttributeValueList[0]
		switch op {
		case "EQ":
			sk.op = "="
		case "LT":
			sk.op = "<"
		case "LE":
			sk.op = "<="
		case "GT":
			sk.op = ">"
		case "GE":
			sk.op = ">="
		case "BEGINS_WITH":
			sk.op = "begins_with"
		}
		sk.value1 = valueToken1
	case "BETWEEN":
		if len(rangeCond.AttributeValueList) != 2 {
			return keyExprToken{}, nil, expressionValues, fmt.Errorf("sort key condition BETWEEN requires exactly two values")
		}
		expressionValues[valueToken1] = rangeCond.AttributeValueList[0]
		valueToken2 := nextLegacyToken(expressionValues, ":__legacy_sk")
		expressionValues[valueToken2] = rangeCond.AttributeValueList[1]
		sk.op = "BETWEEN"
		sk.value1 = valueToken1
		sk.value2 = valueToken2
	default:
		return keyExprToken{}, nil, expressionValues, fmt.Errorf("unsupported sort key condition")
	}

	return pk, sk, expressionValues, nil
}

func scanSegmentForPK(serializedPK string, totalSegments int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(serializedPK))
	return int(h.Sum32() % uint32(totalSegments))
}

func nextLegacyToken(values map[string]any, prefix string) string {
	if _, exists := values[prefix]; !exists {
		return prefix
	}
	for i := 1; ; i++ {
		token := fmt.Sprintf("%s_%d", prefix, i)
		if _, exists := values[token]; !exists {
			return token
		}
	}
}

func expressionValidationMessage(expressionName string, err error) string {
	msg := err.Error()
	if strings.HasPrefix(msg, "missing expression value ") {
		token := strings.TrimPrefix(msg, "missing expression value ")
		token = strings.Trim(token, "\"")
		return "Invalid " + expressionName + ": An expression attribute value used in expression is not defined; attribute value: " + token
	}
	if strings.HasPrefix(msg, "missing expression name ") {
		token := strings.TrimPrefix(msg, "missing expression name ")
		token = strings.Trim(token, "\"")
		return "Invalid " + expressionName + ": An expression attribute name used in the document path is not defined; attribute name: " + token
	}
	return msg
}

func filterExpressionValidationMessage(err error) string {
	return expressionValidationMessage("FilterExpression", err)
}

func conditionExpressionValidationMessage(err error) string {
	return expressionValidationMessage("ConditionExpression", err)
}
