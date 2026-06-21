package main

import (
	"os"
	"testing"
)

func TestPrepareStageRuntimeWritesParamsFromGrpc(t *testing.T) {
	t.Setenv("TORV_RUNTIME_DIR", t.TempDir())

	paramsJSON := `{"accessKeyId":"AKIA","secretAccessKey":"secret"}`
	pp, ip, err := prepareStageRuntime("run1", paramsJSON, "")
	if err != nil {
		t.Fatal(err)
	}
	defer removeStageRuntimeDir("run1")

	pb, _ := os.ReadFile(pp)
	if string(pb) != paramsJSON {
		t.Fatalf("params file = %q, want %q", pb, paramsJSON)
	}
	ib, _ := os.ReadFile(ip)
	if string(ib) != "{}" {
		t.Fatalf("inputs file = %q, want {}", ib)
	}
}

func TestPrepareStageRuntimeRejectsMissingParams(t *testing.T) {
	t.Setenv("TORV_RUNTIME_DIR", t.TempDir())

	_, _, err := prepareStageRuntime("run2", "", "")
	if err == nil {
		t.Fatal("expected error for missing params_json")
	}
}
