package expr

import "testing"

func TestContainsSupportsListOperands(t *testing.T) {
	item := map[string]any{
		"tags": map[string]any{"L": []any{
			map[string]any{"S": "a"},
			map[string]any{"S": "b"},
		}},
	}
	ok, err := Evaluate("contains(tags, :tag)", item, nil, map[string]any{":tag": map[string]any{"S": "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected contains(tags, :tag) to match list member")
	}
}

func TestContainsSupportsStringSetOperands(t *testing.T) {
	item := map[string]any{
		"tags": map[string]any{"SS": []any{"a", "b"}},
	}
	ok, err := Evaluate("contains(tags, :tag)", item, nil, map[string]any{":tag": map[string]any{"S": "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected contains(tags, :tag) to match string set member")
	}
}

func TestContainsSupportsNumberSetOperands(t *testing.T) {
	item := map[string]any{
		"nums": map[string]any{"NS": []any{"1", "2", "3"}},
	}
	ok, err := Evaluate("contains(nums, :n)", item, nil, map[string]any{":n": map[string]any{"N": "2"}})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected contains(nums, :n) to match number set member")
	}
}
