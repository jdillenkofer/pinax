package httpapi

import "testing"

func TestUpdateExpressionSetRemoveAdd(t *testing.T) {
	plan, err := parseUpdateExpression("SET #n = :name, visits = visits + :inc REMOVE old ADD score :add", map[string]string{"#n": "name"}, map[string]any{
		":name": map[string]any{"S": "Jane"},
		":inc":  map[string]any{"N": "1"},
		":add":  map[string]any{"N": "2"},
	})
	if err != nil {
		t.Fatal(err)
	}

	current := map[string]any{
		"visits": map[string]any{"N": "3"},
		"score":  map[string]any{"N": "10"},
		"old":    map[string]any{"S": "x"},
	}
	next, _, err := applyUpdatePlan(current, plan)
	if err != nil {
		t.Fatal(err)
	}
	if next["name"].(map[string]any)["S"] != "Jane" {
		t.Fatal("expected name to be set")
	}
	if next["visits"].(map[string]any)["N"] != "4" {
		t.Fatal("expected visits increment")
	}
	if next["score"].(map[string]any)["N"] != "12" {
		t.Fatal("expected score add")
	}
	if _, ok := next["old"]; ok {
		t.Fatal("expected old removed")
	}
}

func TestApplyProjection(t *testing.T) {
	item := map[string]any{
		"pk":   map[string]any{"S": "u#1"},
		"name": map[string]any{"S": "Jane"},
		"age":  map[string]any{"N": "30"},
	}
	projected, err := applyProjection(item, "pk, #n", map[string]string{"#n": "name"})
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) != 2 {
		t.Fatalf("expected 2 projected attrs, got %d", len(projected))
	}
}

func TestApplyProjectionMissingExpressionName(t *testing.T) {
	item := map[string]any{"pk": map[string]any{"S": "u#1"}}
	_, err := applyProjection(item, "#missing", map[string]string{})
	if err == nil {
		t.Fatal("expected missing expression name validation error")
	}
}
