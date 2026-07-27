package node

import (
	"os"
	"testing"
)

func TestResolveAPIKeyFromEnv(t *testing.T) {
	t.Setenv("TEST_OPENAI_KEY", "sk-test-123")
	key, err := resolveAPIKey(map[string]interface{}{
		"api_key_env": "TEST_OPENAI_KEY",
	})
	if err != nil || key != "sk-test-123" {
		t.Fatalf("expected env key, got %q err=%v", key, err)
	}
}

func TestResolveAPIKeyFromConfig(t *testing.T) {
	key, err := resolveAPIKey(map[string]interface{}{
		"api_key": "inline-key",
	})
	if err != nil || key != "inline-key" {
		t.Fatalf("expected inline key, got %q err=%v", key, err)
	}
}

func TestResolveAPIKeyMissing(t *testing.T) {
	_, err := resolveAPIKey(map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestResolveAPIKeyEnvEmpty(t *testing.T) {
	os.Unsetenv("MISSING_AI_KEY")
	_, err := resolveAPIKey(map[string]interface{}{
		"api_key_env": "MISSING_AI_KEY",
	})
	if err == nil {
		t.Fatal("expected error for unset env")
	}
}
