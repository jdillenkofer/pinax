package expr

import (
	"testing"

	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestEvaluateWithAndAndComparators(t *testing.T) {
	testutils.SkipIfIntegration(t)

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
	testutils.SkipIfIntegration(t)

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

func TestEvaluateWithOrNotAndParens(t *testing.T) {
	testutils.SkipIfIntegration(t)

	item := map[string]any{
		"status": map[string]any{"S": "inactive"},
		"age":    map[string]any{"N": "19"},
	}
	values := map[string]any{
		":active": map[string]any{"S": "active"},
		":age":    map[string]any{"N": "18"},
	}

	ok, err := Evaluate("(status = :active OR age >= :age) AND NOT attribute_not_exists(status)", item, nil, values)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected expression to evaluate to true")
	}
}

func TestEvaluateInAndNotEqual(t *testing.T) {
	testutils.SkipIfIntegration(t)

	item := map[string]any{"tier": map[string]any{"S": "gold"}}
	values := map[string]any{
		":silver": map[string]any{"S": "silver"},
		":gold":   map[string]any{"S": "gold"},
		":bronze": map[string]any{"S": "bronze"},
	}

	ok, err := Evaluate("tier IN (:silver, :gold, :bronze) AND tier <> :bronze", item, nil, values)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected IN/<> expression to evaluate to true")
	}
}

func TestEvaluateNestedPath(t *testing.T) {
	testutils.SkipIfIntegration(t)

	item := map[string]any{
		"profile": map[string]any{"M": map[string]any{
			"address": map[string]any{"M": map[string]any{
				"city": map[string]any{"S": "Berlin"},
			}},
		}},
	}
	values := map[string]any{":city": map[string]any{"S": "Berlin"}}

	ok, err := Evaluate("profile.address.city = :city", item, nil, values)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected nested path evaluation to be true")
	}
}

func TestEvaluateBetweenAndSize(t *testing.T) {
	testutils.SkipIfIntegration(t)

	item := map[string]any{
		"pk":   map[string]any{"S": "u#1"},
		"name": map[string]any{"S": "Jane"},
		"tags": map[string]any{"L": []any{map[string]any{"S": "alpha"}, map[string]any{"S": "beta"}}},
	}
	values := map[string]any{
		":min": map[string]any{"N": "3"},
		":max": map[string]any{"N": "5"},
		":len": map[string]any{"N": "2"},
	}

	ok, err := Evaluate("size(name) BETWEEN :min AND :max AND size(tags) = :len", item, nil, values)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected size/between expression to evaluate to true")
	}
}

func TestEvaluateWithMixedCaseKeywordsAndFunctions(t *testing.T) {
	testutils.SkipIfIntegration(t)

	item := map[string]any{
		"profile": map[string]any{"M": map[string]any{
			"address": map[string]any{"M": map[string]any{
				"city": map[string]any{"S": "Berlin"},
			}},
		}},
	}
	values := map[string]any{
		":city": map[string]any{"S": "Berlin"},
		":n":    map[string]any{"N": "5"},
	}

	ok, err := Evaluate("ATTRIBUTE_EXISTS(profile.address.city) aNd BEGINS_WITH(profile.address.city, :city) AND NOT (SiZe(profile.address.city) < :n)", item, nil, values)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected mixed-case expression to evaluate to true")
	}
}
