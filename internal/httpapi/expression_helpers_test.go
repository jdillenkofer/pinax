package httpapi

import (
	"strings"
	"testing"
)

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

func TestUpdateExpressionListAppendAndIfNotExists(t *testing.T) {
	plan, err := parseUpdateExpression("SET tags = list_append(if_not_exists(tags, :empty), :new)", nil, map[string]any{
		":empty": map[string]any{"L": []any{}},
		":new":   map[string]any{"L": []any{map[string]any{"S": "blue"}, map[string]any{"S": "green"}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	next, _, err := applyUpdatePlan(map[string]any{}, plan)
	if err != nil {
		t.Fatal(err)
	}

	list, ok := next["tags"].(map[string]any)["L"].([]any)
	if !ok {
		t.Fatal("expected tags list")
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(list))
	}

	plan2, err := parseUpdateExpression("SET tags = list_append(tags, :extra)", nil, map[string]any{
		":extra": map[string]any{"L": []any{map[string]any{"S": "red"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	next2, _, err := applyUpdatePlan(next, plan2)
	if err != nil {
		t.Fatal(err)
	}
	list2 := next2["tags"].(map[string]any)["L"].([]any)
	if len(list2) != 3 {
		t.Fatalf("expected 3 tags after append, got %d", len(list2))
	}
}

func TestUpdateExpressionMissingAttributeValueUsesDynamoStyleMessage(t *testing.T) {
	_, err := parseUpdateExpression("SET #v = :missing", map[string]string{"#v": "version"}, map[string]any{
		":other": map[string]any{"N": "1"},
	})
	if err == nil {
		t.Fatal("expected missing expression value error")
	}
	if !strings.Contains(err.Error(), "Invalid UpdateExpression") {
		t.Fatalf("expected Dynamo-style update expression prefix, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), ":missing") {
		t.Fatalf("expected missing token in message, got %q", err.Error())
	}
}

func TestUpdateExpressionListAppendRejectsInvalidArity(t *testing.T) {
	_, err := parseUpdateExpression("SET tags = list_append(:only_one)", nil, map[string]any{
		":only_one": map[string]any{"L": []any{}},
	})
	if err == nil {
		t.Fatal("expected invalid list_append arity error")
	}
	if !strings.Contains(err.Error(), "expected two arguments") {
		t.Fatalf("expected parseTwoArgs error, got %q", err.Error())
	}
}

func TestUpdateExpressionListAppendRejectsNonListOperand(t *testing.T) {
	plan, err := parseUpdateExpression("SET tags = list_append(if_not_exists(tags, :empty), :bad)", nil, map[string]any{
		":empty": map[string]any{"L": []any{}},
		":bad":   map[string]any{"N": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = applyUpdatePlan(map[string]any{}, plan)
	if err == nil {
		t.Fatal("expected non-list operand validation error")
	}
	if !strings.Contains(err.Error(), "list_append supports only L operands") {
		t.Fatalf("expected list operand message, got %q", err.Error())
	}
}

func TestUpdateExpressionDeleteRemovesSetMembers(t *testing.T) {
	plan, err := parseUpdateExpression("DELETE tags :remove", nil, map[string]any{
		":remove": map[string]any{"SS": []any{"blue"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	next, _, err := applyUpdatePlan(map[string]any{
		"tags": map[string]any{"SS": []any{"blue", "green"}},
	}, plan)
	if err != nil {
		t.Fatal(err)
	}

	set, ok := next["tags"].(map[string]any)["SS"].([]any)
	if !ok {
		t.Fatal("expected SS set in result")
	}
	if len(set) != 1 || set[0] != "green" {
		t.Fatalf("expected only green to remain, got %#v", set)
	}
}

func TestUpdateExpressionDeleteDropsAttributeWhenSetBecomesEmpty(t *testing.T) {
	plan, err := parseUpdateExpression("DELETE tags :remove", nil, map[string]any{
		":remove": map[string]any{"SS": []any{"blue", "green"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	next, _, err := applyUpdatePlan(map[string]any{
		"tags": map[string]any{"SS": []any{"blue", "green"}},
	}, plan)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := next["tags"]; ok {
		t.Fatal("expected tags attribute to be removed when set is empty")
	}
}

func TestUpdateExpressionDeleteRejectsMismatchedSetTypes(t *testing.T) {
	plan, err := parseUpdateExpression("DELETE score :remove", nil, map[string]any{
		":remove": map[string]any{"SS": []any{"a"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = applyUpdatePlan(map[string]any{
		"score": map[string]any{"NS": []any{"1", "2"}},
	}, plan)
	if err == nil {
		t.Fatal("expected mismatched set type error")
	}
	if !strings.Contains(err.Error(), "same set type") {
		t.Fatalf("expected mismatched set type message, got %q", err.Error())
	}
}
