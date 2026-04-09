package model

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

const NoSortKey = "__PINAX_NO_SORT_KEY__"

const (
	TableStatusCreating = "CREATING"
	TableStatusActive   = "ACTIVE"
	TableStatusUpdating = "UPDATING"
	TableStatusDeleting = "DELETING"

	BackupStatusCreating  = "CREATING"
	BackupStatusAvailable = "AVAILABLE"
	BackupStatusDeleted   = "DELETED"
	BackupTypeUser        = "USER"

	IndexStatusActive   = "ACTIVE"
	IndexStatusCreating = "CREATING"
	IndexStatusDeleting = "DELETING"

	TTLStatusEnabled   = "ENABLED"
	TTLStatusEnabling  = "ENABLING"
	TTLStatusDisabled  = "DISABLED"
	TTLStatusDisabling = "DISABLING"

	PointInTimeRecoveryStatusEnabled  = "ENABLED"
	PointInTimeRecoveryStatusDisabled = "DISABLED"
	ContinuousBackupsStatusEnabled    = "ENABLED"
)

type TimeToLive struct {
	Enabled  bool
	AttrName string
	Status   string
	StatusAt int64
}

type PointInTimeRecovery struct {
	Enabled              bool
	RecoveryPeriodInDays int64
	EnabledAt            int64
	LatestRestorableAt   int64
	EarliestRestorableAt int64
}

type Backup struct {
	BackupARN                 string
	BackupName                string
	TableName                 string
	TableARN                  string
	TableID                   string
	BackupStatus              string
	BackupType                string
	BackupCreationDateTime    int64
	BackupSizeBytes           int64
	SourceTableDetails        map[string]any
	SourceTableFeatureDetails map[string]any
	SnapshotTable             Table
	SnapshotItems             []map[string]any
}

type Table struct {
	Name               string
	HashKey            string
	HashType           string
	RangeKey           string
	RangeType          string
	BillingMode        string
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
	TableClass         string
	DeletionProtection bool
	Status             string
	StatusAt           int64
	GSIs               []GlobalSecondaryIndex
	LSIs               []LocalSecondaryIndex
	Stream             StreamSpecification
	SSE                SSESpecification
	Tags               []Tag
	CreatedAt          int64

	TimeToLive TimeToLive
	PITR       PointInTimeRecovery
}

type ItemChange struct {
	TableName  string
	PK         string
	SK         string
	ChangeType string
	Item       map[string]any
	ChangedAt  int64
	Sequence   int64
}

type ItemChangeCursor struct {
	Found     bool
	ChangedAt int64
	Sequence  int64
}

type PITRCheckpointItem struct {
	PK   string
	SK   string
	Item map[string]any
}

type PITRCheckpoint struct {
	Found     bool
	ChangedAt int64
	Sequence  int64
	Items     []PITRCheckpointItem
}

type StreamSpecification struct {
	Enabled  bool
	ViewType string
	ARN      string
	Label    string
}

type SSESpecification struct {
	Enabled        bool
	SSEType        string
	Status         string
	KMSMasterKeyID string
}

type Tag struct {
	Key   string
	Value string
}

type TransactWriteIdempotencyRecord struct {
	Token       string
	RequestHash string
	Response    map[string]any
	CreatedAt   int64
	ExpiresAt   int64
}

type GlobalSecondaryIndex struct {
	IndexName      string
	HashKey        string
	HashType       string
	RangeKey       string
	RangeType      string
	Status         string
	StatusAt       int64
	ReadCapacity   int64
	WriteCapacity  int64
	ProjectionType string
	NonKeyAttrs    []string
}

type LocalSecondaryIndex struct {
	IndexName      string
	RangeKey       string
	RangeType      string
	ProjectionType string
	NonKeyAttrs    []string
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
			"IndexName":      g.IndexName,
			"KeySchema":      keySchema,
			"IndexStatus":    firstNonEmpty(g.Status, IndexStatusActive),
			"IndexSizeBytes": int64(0),
			"ItemCount":      int64(0),
			"IndexArn":       localIndexARN(t.Name, g.IndexName),
		})
		if g.ReadCapacity > 0 || g.WriteCapacity > 0 {
			gsis[len(gsis)-1]["ProvisionedThroughput"] = map[string]any{
				"NumberOfDecreasesToday": 0,
				"LastIncreaseDateTime":   0,
				"LastDecreaseDateTime":   0,
				"ReadCapacityUnits":      g.ReadCapacity,
				"WriteCapacityUnits":     g.WriteCapacity,
			}
		}
		projection := map[string]any{"ProjectionType": g.ProjectionType}
		if g.ProjectionType == "INCLUDE" {
			projection["NonKeyAttributes"] = g.NonKeyAttrs
		}
		gsis[len(gsis)-1]["Projection"] = projection
	}

	lsis := make([]map[string]any, 0, len(t.LSIs))
	for _, l := range t.LSIs {
		projection := map[string]any{"ProjectionType": l.ProjectionType}
		if l.ProjectionType == "INCLUDE" {
			projection["NonKeyAttributes"] = l.NonKeyAttrs
		}
		lsis = append(lsis, map[string]any{
			"IndexName": l.IndexName,
			"KeySchema": []map[string]any{
				{"AttributeName": t.HashKey, "KeyType": "HASH"},
				{"AttributeName": l.RangeKey, "KeyType": "RANGE"},
			},
			"Projection":     projection,
			"IndexStatus":    "ACTIVE",
			"IndexSizeBytes": int64(0),
			"ItemCount":      int64(0),
			"IndexArn":       localIndexARN(t.Name, l.IndexName),
		})
	}

	desc := map[string]any{
		"AttributeDefinitions": t.AttributeDefinitions(),
		"TableName":            t.Name,
		"TableArn":             localTableARN(t.Name),
		"TableId":              localTableID(t.Name),
		"KeySchema":            t.KeySchema(),
		"TableStatus":          firstNonEmpty(t.Status, TableStatusActive),
		"CreationDateTime":     t.CreatedAt,
		"ItemCount":            itemCount,
		"TableSizeBytes":       0,
		"ProvisionedThroughput": map[string]any{
			"NumberOfDecreasesToday": 0,
			"LastIncreaseDateTime":   0,
			"LastDecreaseDateTime":   0,
			"ReadCapacityUnits":      t.ReadCapacityUnits,
			"WriteCapacityUnits":     t.WriteCapacityUnits,
		},
		"BillingModeSummary":        map[string]any{"BillingMode": firstNonEmpty(t.BillingMode, "PAY_PER_REQUEST")},
		"GlobalSecondaryIndexes":    gsis,
		"LocalSecondaryIndexes":     lsis,
		"TableClassSummary":         map[string]any{"TableClass": firstNonEmpty(t.TableClass, "STANDARD"), "LastUpdateDateTime": t.CreatedAt},
		"DeletionProtectionEnabled": t.DeletionProtection,
	}

	if t.Stream.Enabled {
		desc["StreamSpecification"] = map[string]any{
			"StreamEnabled":  true,
			"StreamViewType": t.Stream.ViewType,
		}
		if strings.TrimSpace(t.Stream.ARN) != "" {
			desc["LatestStreamArn"] = t.Stream.ARN
		}
		if strings.TrimSpace(t.Stream.Label) != "" {
			desc["LatestStreamLabel"] = t.Stream.Label
		}
	}

	if t.SSE.Enabled || strings.TrimSpace(t.SSE.Status) != "" || strings.TrimSpace(t.SSE.SSEType) != "" || strings.TrimSpace(t.SSE.KMSMasterKeyID) != "" {
		sse := map[string]any{
			"Status": firstNonEmpty(t.SSE.Status, "DISABLED"),
		}
		if strings.TrimSpace(t.SSE.SSEType) != "" {
			sse["SSEType"] = t.SSE.SSEType
		}
		if strings.TrimSpace(t.SSE.KMSMasterKeyID) != "" {
			sse["KMSMasterKeyArn"] = t.SSE.KMSMasterKeyID
		}
		desc["SSEDescription"] = sse
	}

	if t.TimeToLive.Enabled || strings.TrimSpace(t.TimeToLive.Status) != "" || strings.TrimSpace(t.TimeToLive.AttrName) != "" {
		desc["TimeToLive"] = map[string]any{
			"TimeToLiveStatus": firstNonEmpty(t.TimeToLive.Status, TTLStatusEnabled),
			"AttributeName":    t.TimeToLive.AttrName,
		}
	}

	return desc
}

func (t Table) GetGSI(indexName string) (GlobalSecondaryIndex, bool) {
	for _, g := range t.GSIs {
		if g.IndexName == indexName {
			return g, true
		}
	}
	return GlobalSecondaryIndex{}, false
}

func firstNonEmpty(v, fallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	return v
}

func localTableARN(tableName string) string {
	return "arn:aws:dynamodb:local:000000000000:table/" + tableName
}

func localIndexARN(tableName string, indexName string) string {
	return localTableARN(tableName) + "/index/" + indexName
}

func localTableID(tableName string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tableName))
	sum := h.Sum64()
	return fmt.Sprintf("00000000-0000-0000-%04x-%012x", sum>>48, sum&0x0000ffffffffffff)
}

func AttributeValueType(v any) string {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return ""
	}
	for k := range m {
		return k
	}
	return ""
}

func ValidateKeyAttributeType(v any, expectedType string, attrName string) error {
	if strings.TrimSpace(expectedType) == "" {
		return nil
	}
	actual := AttributeValueType(v)
	if actual == "" {
		return fmt.Errorf("invalid key attribute %q", attrName)
	}
	if actual != expectedType {
		return fmt.Errorf("One or more parameter values were invalid: Type mismatch for key")
	}
	return nil
}

func ValidateSecondaryIndexKeyTypes(t Table, item map[string]any) error {
	for _, g := range t.GSIs {
		if g.HashKey != "" {
			if v, ok := item[g.HashKey]; ok {
				if err := ValidateKeyAttributeType(v, g.HashType, g.HashKey); err != nil {
					return err
				}
			}
		}
		if g.RangeKey != "" {
			if v, ok := item[g.RangeKey]; ok {
				if err := ValidateKeyAttributeType(v, g.RangeType, g.RangeKey); err != nil {
					return err
				}
			}
		}
	}
	for _, l := range t.LSIs {
		if l.RangeKey != "" {
			if v, ok := item[l.RangeKey]; ok {
				if err := ValidateKeyAttributeType(v, l.RangeType, l.RangeKey); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (t Table) GetLSI(indexName string) (LocalSecondaryIndex, bool) {
	for _, l := range t.LSIs {
		if l.IndexName == indexName {
			return l, true
		}
	}
	return LocalSecondaryIndex{}, false
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
	if err := ValidateKeyAttributeType(pv, t.HashType, t.HashKey); err != nil {
		return "", "", err
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
	if err := ValidateKeyAttributeType(sv, t.RangeType, t.RangeKey); err != nil {
		return "", "", err
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
	if err := ValidateKeyAttributeType(pv, t.HashType, t.HashKey); err != nil {
		return "", "", err
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
	if err := ValidateKeyAttributeType(sv, t.RangeType, t.RangeKey); err != nil {
		return "", "", err
	}
	return pk, sk, nil
}

func ExtractGSIKeys(g GlobalSecondaryIndex, item map[string]any) (pk string, sk string, ok bool) {
	pv, ok := item[g.HashKey]
	if !ok {
		return "", "", false
	}
	pk, err := SerializeKeyValue(pv)
	if err != nil {
		return "", "", false
	}
	if g.RangeKey == "" {
		return pk, NoSortKey, true
	}
	sv, ok := item[g.RangeKey]
	if !ok {
		// DynamoDB GSIs are sparse: if sort key is missing, item is not in GSI
		return "", "", false
	}
	sk, err = SerializeKeyValue(sv)
	if err != nil {
		return "", "", false
	}
	return pk, sk, true
}

func ExtractTTL(t Table, item map[string]any) (int64, bool) {
	if !t.TimeToLive.Enabled || t.TimeToLive.AttrName == "" {
		return 0, false
	}
	v, ok := item[t.TimeToLive.AttrName]
	if !ok {
		return 0, false
	}
	m, ok := v.(map[string]any)
	if !ok {
		return 0, false
	}
	ns, ok := m["N"].(string)
	if !ok {
		return 0, false
	}
	ttl, err := strconv.ParseInt(ns, 10, 64)
	if err != nil {
		return 0, false
	}
	return ttl, true
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
