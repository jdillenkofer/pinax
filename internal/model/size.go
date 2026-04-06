package model

import (
	"encoding/json"
)

const MaxItemSize = 400 * 1024

func CalculateItemSizeBytes(item map[string]any) int {
	data, _ := json.Marshal(item)
	return len(data)
}

func ItemTooLarge(item map[string]any) bool {
	return CalculateItemSizeBytes(item) > MaxItemSize
}
