package model

import (
	"fmt"
	"strings"
)

const NoSortKey = "__PINAX_NO_SORT_KEY__"

type Table struct {
	Name      string
	HashKey   string
	HashType  string
	RangeKey  string
	RangeType string
	GSIs      []GlobalSecondaryIndex
	CreatedAt int64
}

type GlobalSecondaryIndex struct {
	IndexName      string
	HashKey        string
	HashType       string
	RangeKey       string
	RangeType      string
	ProjectionType string
}

func (t Table) AttributeDefinitions() []map[string]any {
	defs := []map[string]any{{"AttributeName": t.HashKey, "AttributeType": t.HashType}}
	if t.RangeKey != "" {
		defs = append(defs, map[string]any{"AttributeName": t.RangeKey, "AttributeType": t.RangeType})
	}
	return defs
}

func (t Table) KeySchema() []map[string]any {
	ks := []map[string]any{{"AttributeName": t.HashKey, "KeyType": "HASH"}}
	if t.RangeKey != "" {
		ks = append(ks, map[string]any{"AttributeName": t.RangeKey, "KeyType": "RANGE"})
	}
	return ks
}

func (t Table) Description(itemCount int64) map[string]any {
	gsis := make([]map[string]any, 0, len(t.GSIs))
	for _, g := range t.GSIs {
		keySchema := []map[string]any{{"AttributeName": g.HashKey, "KeyType": "HASH"}}
		if g.RangeKey != "" {
			keySchema = append(keySchema, map[string]any{"AttributeName": g.RangeKey, "KeyType": "RANGE"})
		}
		gsis = append(gsis, map[string]any{
			"IndexName":   g.IndexName,
			"KeySchema":   keySchema,
			"Projection":  map[string]any{"ProjectionType": g.ProjectionType},
			"IndexStatus": "ACTIVE",
		})
	}

	return map[string]any{
		"AttributeDefinitions": t.AttributeDefinitions(),
		"TableName":            t.Name,
		"KeySchema":            t.KeySchema(),
		"TableStatus":          "ACTIVE",
		"CreationDateTime":     t.CreatedAt,
		"ItemCount":            itemCount,
		"TableSizeBytes":       0,
		"ProvisionedThroughput": map[string]any{
			"NumberOfDecreasesToday": 0,
			"ReadCapacityUnits":      0,
			"WriteCapacityUnits":     0,
		},
		"BillingModeSummary":     map[string]any{"BillingMode": "PAY_PER_REQUEST"},
		"GlobalSecondaryIndexes": gsis,
	}
}

func (t Table) GetGSI(indexName string) (GlobalSecondaryIndex, bool) {
	for _, g := range t.GSIs {
		if g.IndexName == indexName {
			return g, true
		}
	}
	return GlobalSecondaryIndex{}, false
}

func ExtractItemKeys(t Table, item map[string]any) (pk string, sk string, err error) {
	pv, ok := item[t.HashKey]
	if !ok {
		return "", "", fmt.Errorf("missing partition key attribute %q", t.HashKey)
	}
	pk, err = SerializeKeyValue(pv)
	if err != nil {
		return "", "", fmt.Errorf("invalid partition key %q: %w", t.HashKey, err)
	}
	if t.RangeKey == "" {
		return pk, NoSortKey, nil
	}
	sv, ok := item[t.RangeKey]
	if !ok {
		return "", "", fmt.Errorf("missing sort key attribute %q", t.RangeKey)
	}
	sk, err = SerializeKeyValue(sv)
	if err != nil {
		return "", "", fmt.Errorf("invalid sort key %q: %w", t.RangeKey, err)
	}
	return pk, sk, nil
}

func ExtractKey(t Table, key map[string]any) (pk string, sk string, err error) {
	pv, ok := key[t.HashKey]
	if !ok {
		return "", "", fmt.Errorf("missing partition key attribute %q", t.HashKey)
	}
	pk, err = SerializeKeyValue(pv)
	if err != nil {
		return "", "", fmt.Errorf("invalid partition key %q: %w", t.HashKey, err)
	}
	if t.RangeKey == "" {
		return pk, NoSortKey, nil
	}
	sv, ok := key[t.RangeKey]
	if !ok {
		return "", "", fmt.Errorf("missing sort key attribute %q", t.RangeKey)
	}
	sk, err = SerializeKeyValue(sv)
	if err != nil {
		return "", "", fmt.Errorf("invalid sort key %q: %w", t.RangeKey, err)
	}
	return pk, sk, nil
}

func SerializeKeyValue(v any) (string, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return "", fmt.Errorf("attribute value must be object")
	}
	if s, ok := m["S"]; ok {
		ss, ok := s.(string)
		if !ok {
			return "", fmt.Errorf("S must be string")
		}
		return "S|" + ss, nil
	}
	if n, ok := m["N"]; ok {
		ns, ok := n.(string)
		if !ok {
			return "", fmt.Errorf("N must be string")
		}
		return "N|" + ns, nil
	}
	if b, ok := m["B"]; ok {
		bs, ok := b.(string)
		if !ok {
			return "", fmt.Errorf("B must be base64 string")
		}
		return "B|" + bs, nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return "", fmt.Errorf("unsupported key type(s): %s", strings.Join(keys, ","))
}
