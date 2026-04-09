package expr

import "testing"

func TestParseKeyConditionWithSortBetween(t *testing.T) {
	parsed, err := ParseKeyCondition("#pk = :pk AND #sk BETWEEN :a AND :b")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Partition.Attribute != "#pk" || parsed.Partition.Value1 != ":pk" {
		t.Fatalf("unexpected partition token: %+v", parsed.Partition)
	}
	if parsed.Sort == nil {
		t.Fatal("expected sort condition")
	}
	if parsed.Sort.Operator != "BETWEEN" || parsed.Sort.Attribute != "#sk" || parsed.Sort.Value1 != ":a" || parsed.Sort.Value2 != ":b" {
		t.Fatalf("unexpected sort token: %+v", parsed.Sort)
	}
}

func TestParseKeyConditionRequiresPartitionEquals(t *testing.T) {
	_, err := ParseKeyCondition("pk >= :pk")
	if err == nil {
		t.Fatal("expected partition condition validation error")
	}
}

func TestParseKeyConditionBeginsWithMixedCase(t *testing.T) {
	parsed, err := ParseKeyCondition("pk = :pk AnD BeGiNs_WiTh(sk, :prefix)")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Sort == nil || parsed.Sort.Operator != "begins_with" {
		t.Fatalf("expected begins_with sort condition, got %+v", parsed.Sort)
	}
}
