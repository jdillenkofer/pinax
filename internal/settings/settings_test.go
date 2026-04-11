package settings

import (
	"strings"
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

func TestLoadSettingsRejectsInvalidCredentialAccountID(t *testing.T) {
	t.Setenv("PINAX_CREDENTIALS_0_ACCESS_KEY_ID", "akid")
	t.Setenv("PINAX_CREDENTIALS_0_SECRET_ACCESS_KEY", "secret")
	t.Setenv("PINAX_CREDENTIALS_0_ACCOUNT_ID", "1234")
	t.Setenv("PINAX_CREDENTIALS_1_ACCESS_KEY_ID", "")
	t.Setenv("PINAX_CREDENTIALS_1_SECRET_ACCESS_KEY", "")

	_, err := LoadSettings(nil)
	if err == nil {
		t.Fatal("expected error for invalid account id")
	}
	if !strings.Contains(err.Error(), "must be exactly 12 digits") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadSettingsAccepts12DigitCredentialAccountID(t *testing.T) {
	t.Setenv("PINAX_CREDENTIALS_0_ACCESS_KEY_ID", "akid")
	t.Setenv("PINAX_CREDENTIALS_0_SECRET_ACCESS_KEY", "secret")
	t.Setenv("PINAX_CREDENTIALS_0_ACCOUNT_ID", "739182640517")
	t.Setenv("PINAX_CREDENTIALS_1_ACCESS_KEY_ID", "")
	t.Setenv("PINAX_CREDENTIALS_1_SECRET_ACCESS_KEY", "")

	s, err := LoadSettings(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Credentials()) != 1 {
		t.Fatalf("expected one credential, got %d", len(s.Credentials()))
	}
	if s.Credentials()[0].AccountID != "739182640517" {
		t.Fatalf("unexpected account id: %q", s.Credentials()[0].AccountID)
	}
}
