package httpapi

import "strings"

func addConsumedCapacity(resp map[string]any, capacity string, tableName string, readUnits, writeUnits float64) {
	if capacity == "" || capacity == "NONE" {
		return
	}
	if resp["ConsumedCapacity"] == nil {
		resp["ConsumedCapacity"] = []map[string]any{}
	}
	list := resp["ConsumedCapacity"].([]map[string]any)
	entry := map[string]any{
		"TableName": logicalTableNameFromKey(tableName),
	}
	if readUnits > 0 {
		entry["ReadCapacityUnits"] = readUnits
		entry["CapacityUnits"] = readUnits
	}
	if writeUnits > 0 {
		entry["WriteCapacityUnits"] = writeUnits
		entry["CapacityUnits"] = writeUnits
	}
	if readUnits > 0 && writeUnits > 0 {
		entry["CapacityUnits"] = readUnits + writeUnits
	}
	resp["ConsumedCapacity"] = append(list, entry)
}

func setSingleConsumedCapacity(resp map[string]any, capacity string, tableName string, readUnits, writeUnits float64) {
	if capacity == "" || capacity == "NONE" {
		return
	}
	entry := map[string]any{"TableName": logicalTableNameFromKey(tableName)}
	if readUnits > 0 {
		entry["ReadCapacityUnits"] = readUnits
		entry["CapacityUnits"] = readUnits
	}
	if writeUnits > 0 {
		entry["WriteCapacityUnits"] = writeUnits
		entry["CapacityUnits"] = writeUnits
	}
	if readUnits > 0 && writeUnits > 0 {
		entry["CapacityUnits"] = readUnits + writeUnits
	}
	resp["ConsumedCapacity"] = entry
}

func setSingleQueryConsumedCapacity(resp map[string]any, mode string, tableName, indexName, indexType string, readUnits float64) {
	if mode == "" || mode == "NONE" {
		return
	}
	entry := map[string]any{
		"TableName":         logicalTableNameFromKey(tableName),
		"ReadCapacityUnits": readUnits,
		"CapacityUnits":     readUnits,
	}
	if mode == "INDEXES" && strings.TrimSpace(indexName) != "" {
		if indexType == "GSI" {
			entry["GlobalSecondaryIndexes"] = map[string]any{
				indexName: map[string]any{
					"ReadCapacityUnits": readUnits,
					"CapacityUnits":     readUnits,
				},
			}
		} else if indexType == "LSI" {
			entry["LocalSecondaryIndexes"] = map[string]any{
				indexName: map[string]any{
					"ReadCapacityUnits": readUnits,
					"CapacityUnits":     readUnits,
				},
			}
		}
	}
	resp["ConsumedCapacity"] = entry
}

func addQueryConsumedCapacity(resp map[string]any, mode string, tableName, indexName, indexType string, readUnits float64) {
	if mode == "" || mode == "NONE" {
		return
	}
	if resp["ConsumedCapacity"] == nil {
		resp["ConsumedCapacity"] = []map[string]any{}
	}
	entry := map[string]any{
		"TableName":         logicalTableNameFromKey(tableName),
		"ReadCapacityUnits": readUnits,
		"CapacityUnits":     readUnits,
	}
	if mode == "INDEXES" && strings.TrimSpace(indexName) != "" {
		if indexType == "GSI" {
			entry["GlobalSecondaryIndexes"] = map[string]any{
				indexName: map[string]any{
					"ReadCapacityUnits": readUnits,
					"CapacityUnits":     readUnits,
				},
			}
		} else if indexType == "LSI" {
			entry["LocalSecondaryIndexes"] = map[string]any{
				indexName: map[string]any{
					"ReadCapacityUnits": readUnits,
					"CapacityUnits":     readUnits,
				},
			}
		}
	}
	list := resp["ConsumedCapacity"].([]map[string]any)
	resp["ConsumedCapacity"] = append(list, entry)
}
