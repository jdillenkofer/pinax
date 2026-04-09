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
