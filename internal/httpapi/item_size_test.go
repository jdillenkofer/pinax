package httpapi

import (
	"testing"

	"github.com/jdillenkofer/pinax/internal/model"
)

func TestItemSizeCalculation(t *testing.T) {
	smallItem := map[string]any{"pk": "test", "data": "hello"}
	if model.ItemTooLarge(smallItem) {
		t.Error("expected small item to not be too large")
	}

	largeItem := map[string]any{}
	for i := 0; i < 100000; i++ {
		largeItem["attr"+string(rune(i))] = "x"
	}
	if !model.ItemTooLarge(largeItem) {
		t.Error("expected large item to be too large")
	}
}
