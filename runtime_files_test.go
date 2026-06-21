package main

import "testing"

func TestValidateParamsJSON(t *testing.T) {
	if err := validateParamsJSON(`{"accessKeyId":"AKIA"}`); err != nil {
		t.Fatal(err)
	}
	if err := validateParamsJSON(""); err == nil {
		t.Fatal("expected error for empty params")
	}
}
