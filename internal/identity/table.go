package identity

import (
	"fmt"
	"strings"
)

const DefaultLocalAccountID = "000000000000"
const scopedTableKeySeparator = "#"

func ScopedTableKey(accountID string, tableName string) string {
	return strings.TrimSpace(accountID) + scopedTableKeySeparator + strings.TrimSpace(tableName)
}

func SplitScopedTableKey(v string) (string, string) {
	v = strings.TrimSpace(v)
	parts := strings.SplitN(v, scopedTableKeySeparator, 2)
	if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != "" {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return DefaultLocalAccountID, v
}

func LogicalTableNameFromKey(v string) string {
	_, tableName := SplitScopedTableKey(v)
	return tableName
}

func ParseTableARN(v string) (tableName string, accountID string, isARN bool, err error) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "arn:") {
		return strings.TrimSpace(v), "", false, nil
	}
	parts := strings.SplitN(v, ":", 6)
	if len(parts) < 6 {
		return "", "", true, fmt.Errorf("ResourceArn must be a valid ARN")
	}
	accountID = strings.TrimSpace(parts[4])
	resource := strings.TrimSpace(parts[5])
	if !strings.HasPrefix(resource, "table/") {
		return "", "", true, fmt.Errorf("ResourceArn must identify a DynamoDB table")
	}
	remainder := strings.TrimPrefix(resource, "table/")
	if remainder == "" {
		return "", "", true, fmt.Errorf("ResourceArn must identify a DynamoDB table")
	}
	tableName = remainder
	if slash := strings.Index(tableName, "/"); slash >= 0 {
		tableName = tableName[:slash]
	}
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return "", "", true, fmt.Errorf("ResourceArn must identify a DynamoDB table")
	}
	return tableName, accountID, true, nil
}

func NormalizeTableNameIdentifier(v string) string {
	name, _, _, err := ParseTableARN(v)
	if err == nil {
		return name
	}
	return strings.TrimSpace(v)
}

func LocalTableARN(tableName string) string {
	accountID, logicalTableName := SplitScopedTableKey(tableName)
	return "arn:aws:dynamodb:local:" + accountID + ":table/" + logicalTableName
}
