package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Artifact struct {
	ItemId        string `json:"itemId"`
	Name          string `json:"name"`
	Path          string `json:"path"`
	Bytes         int    `json:"bytes"`
	ComputeHostId string `json:"computeHostId"`
}

func writeOutputArtifacts(dataRoot, workspaceID, stageRunID, computeHostID, outputsJSON string) (string, error) {
	raw := strings.TrimSpace(outputsJSON)
	if raw == "" || raw == "null" {
		raw = "{}"
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return "", fmt.Errorf("outputs is not a JSON object: %w", err)
	}

	names := make([]string, 0, len(obj))
	for name := range obj {
		names = append(names, name)
	}
	sort.Strings(names)

	outDir := filepath.Join(dataRoot, workspaceID, "runs", stageRunID, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}

	artifacts := make([]Artifact, 0, len(names))
	for _, name := range names {
		if err := validateOutputName(name); err != nil {
			return "", err
		}
		body := obj[name]
		if len(body) == 0 {
			body = json.RawMessage("null")
		}
		rel := "runs/" + stageRunID + "/out/" + name + ".json"
		if err := os.WriteFile(filepath.Join(outDir, name+".json"), body, 0o644); err != nil {
			return "", err
		}
		artifacts = append(artifacts, Artifact{
			ItemId:        name,
			Name:          name,
			Path:          rel,
			Bytes:         len(body),
			ComputeHostId: computeHostID,
		})
	}

	b, err := json.Marshal(artifacts)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func validateOutputName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\:`) {
		return fmt.Errorf("invalid output name %q", name)
	}
	return nil
}
