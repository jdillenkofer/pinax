package model

import "math"

const ReadUnitSize = 4 * 1024
const WriteUnitSize = 1 * 1024

func CalculateReadCapacityUnits(itemSizeBytes int, consistent bool) float64 {
	if itemSizeBytes <= 0 {
		return 0
	}
	units := math.Ceil(float64(itemSizeBytes) / float64(ReadUnitSize))
	if !consistent {
		return units / 2
	}
	return units
}

func CalculateWriteCapacityUnits(itemSizeBytes int) float64 {
	if itemSizeBytes <= 0 {
		return 0
	}
	return math.Ceil(float64(itemSizeBytes) / float64(WriteUnitSize))
}
