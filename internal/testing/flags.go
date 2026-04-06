package testing

import (
	"flag"
	"testing"
)

var (
	Integration = flag.Bool("integration", false, "run integration tests")
)

func SkipIfIntegration(t *testing.T) {
	if *Integration {
		t.Skip("Skipping unit test when running integration tests")
	}
}

func SkipIfNotIntegration(t *testing.T) {
	if !*Integration {
		t.Skip("Skipping integration test")
	}
}
