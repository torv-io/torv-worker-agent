package main

import (
	"os"
	"testing"
)

func TestValidateParamsJSON(t *testing.T) {
	if err := validateParamsJSON(`{"accessKeyId":"AKIA"}`); err != nil {
		t.Fatal(err)
	}
	if err := validateParamsJSON(""); err == nil {
		t.Fatal("expected error for empty params")
	}
}

func TestPrepareStageInputsEmptyURL(t *testing.T) {
	t.Setenv("TORV_RUNTIME_DIR", t.TempDir())
	path, err := prepareStageInputs("run1", "")
	if err != nil {
		t.Fatal(err)
	}
	defer removeStageRuntimeDir("run1")
	body, _ := os.ReadFile(path)
	if string(body) != "{}" {
		t.Fatalf("got %q", body)
	}
}
