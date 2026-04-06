package settings

import (
	"testing"

	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func addrOf[T any](v T) *T { return &v }

func TestMergeSettingsOverride(t *testing.T) {
	testutils.SkipIfIntegration(t)

	a := &Settings{region: addrOf("eu-central-1")}
	b := &Settings{region: addrOf("us-east-1")}
	merged := mergeSettings(a, b)
	if merged.Region() != "us-east-1" {
		t.Fatalf("expected region us-east-1, got %s", merged.Region())
	}
}

func TestDefaultRegion(t *testing.T) {
	testutils.SkipIfIntegration(t)

	s := &Settings{}
	if s.Region() != "eu-central-1" {
		t.Fatalf("expected default region eu-central-1, got %s", s.Region())
	}
}
