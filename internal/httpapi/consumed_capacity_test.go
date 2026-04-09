package httpapi

import "testing"

func TestAddQueryConsumedCapacityIndexesForGSI(t *testing.T) {
	resp := map[string]any{}
	addQueryConsumedCapacity(resp, "INDEXES", "orders", "status-index", "GSI", 1.5)

	cc, ok := resp["ConsumedCapacity"].([]map[string]any)
	if !ok || len(cc) != 1 {
		t.Fatalf("expected one consumed capacity entry, got %+v", resp["ConsumedCapacity"])
	}
	entry := cc[0]
	gsis, ok := entry["GlobalSecondaryIndexes"].(map[string]any)
	if !ok {
		t.Fatalf("expected GlobalSecondaryIndexes map, got %+v", entry)
	}
	if _, ok := gsis["status-index"]; !ok {
		t.Fatalf("expected status-index attribution, got %+v", gsis)
	}
}

func TestAddQueryConsumedCapacityIndexesForLSI(t *testing.T) {
	resp := map[string]any{}
	addQueryConsumedCapacity(resp, "INDEXES", "orders", "status-lsi", "LSI", 2)

	cc := resp["ConsumedCapacity"].([]map[string]any)
	lsis, ok := cc[0]["LocalSecondaryIndexes"].(map[string]any)
	if !ok {
		t.Fatalf("expected LocalSecondaryIndexes map, got %+v", cc[0])
	}
	if _, ok := lsis["status-lsi"]; !ok {
		t.Fatalf("expected status-lsi attribution, got %+v", lsis)
	}
}

func TestAddQueryConsumedCapacityTotalOmitsIndexBreakdown(t *testing.T) {
	resp := map[string]any{}
	addQueryConsumedCapacity(resp, "TOTAL", "orders", "status-index", "GSI", 3)

	cc := resp["ConsumedCapacity"].([]map[string]any)
	if _, ok := cc[0]["GlobalSecondaryIndexes"]; ok {
		t.Fatalf("did not expect index breakdown for TOTAL mode: %+v", cc[0])
	}
}
