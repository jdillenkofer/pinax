package expr

import "testing"

func TestEvaluateWithAndAndComparators(t *testing.T) {
	item := map[string]any{
		"pk":   map[string]any{"S": "u#1"},
		"age":  map[string]any{"N": "30"},
		"name": map[string]any{"S": "Jane"},
	}
	values := map[string]any{
		":age": map[string]any{"N": "21"},
		":pre": map[string]any{"S": "Ja"},
	}
	ok, err := Evaluate("attribute_exists(pk) AND age >= :age AND begins_with(name, :pre)", item, nil, values)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected expression to evaluate to true")
	}
}

func TestEvaluateContains(t *testing.T) {
	item := map[string]any{"name": map[string]any{"S": "Jane Doe"}}
	values := map[string]any{":q": map[string]any{"S": "Doe"}}
	ok, err := Evaluate("contains(name, :q)", item, nil, values)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected contains to match")
	}
}
