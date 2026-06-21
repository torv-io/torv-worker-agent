package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type runtimePayload struct {
	Params json.RawMessage `json:"params"`
	Inputs json.RawMessage `json:"inputs"`
}

func writeStageRuntimeFiles(stageRunId, contextJSON, paramsJSON, inputsJSON string) (paramsPath, inputsPath string, err error) {
	dir := os.TempDir()
	paramsPath = filepath.Join(dir, fmt.Sprintf("torv-params-%s.json", stageRunId))
	inputsPath = filepath.Join(dir, fmt.Sprintf("torv-inputs-%s.json", stageRunId))

	params := []byte(paramsJSON)
	inputs := []byte(inputsJSON)

	if len(params) == 0 || len(inputs) == 0 {
		var payload runtimePayload
		if unmarshalErr := json.Unmarshal([]byte(contextJSON), &payload); unmarshalErr != nil {
			return "", "", fmt.Errorf("parse context_json: %w", unmarshalErr)
		}
		if len(params) == 0 {
			params = payload.Params
		}
		if len(inputs) == 0 {
			inputs = payload.Inputs
		}
	}

	if len(params) == 0 {
		params = []byte("{}")
	}
	if len(inputs) == 0 {
		inputs = []byte("{}")
	}

	if err := os.WriteFile(paramsPath, params, 0o600); err != nil {
		return "", "", fmt.Errorf("write params file: %w", err)
	}
	if err := os.WriteFile(inputsPath, inputs, 0o600); err != nil {
		_ = os.Remove(paramsPath)
		return "", "", fmt.Errorf("write inputs file: %w", err)
	}

	return paramsPath, inputsPath, nil
}
