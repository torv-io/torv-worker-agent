package main

import "testing"

func TestSafeArtifactPath(t *testing.T) {
	ok, err := safeArtifactPath("runs/18e13bb36df516/out/datasetIds.json")
	if err != nil || ok != "runs/18e13bb36df516/out/datasetIds.json" {
		t.Fatalf("got %q %v", ok, err)
	}
	for _, bad := range []string{
		"../runs/x/out/a.json",
		"/data/runs/x/out/a.json",
		"runs/x/out/../secret",
		"runs/x/out/a.json ",
	} {
		if _, err := safeArtifactPath(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}
