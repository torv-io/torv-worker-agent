package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func runtimeDataDir() string {
	if d := strings.TrimSpace(os.Getenv("TORV_RUNTIME_DIR")); d != "" {
		return d
	}
	return os.TempDir()
}

func prepareStageRuntime(stageRunId, paramsJSON, inputsPresignedURL string) (paramsPath, inputsPath string, err error) {
	paramsJSON = strings.TrimSpace(paramsJSON)
	if paramsJSON == "" {
		return "", "", fmt.Errorf("params_json missing on work item")
	}
	var paramsCheck map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal([]byte(paramsJSON), &paramsCheck); unmarshalErr != nil {
		return "", "", fmt.Errorf("params_json is not valid JSON object: %w", unmarshalErr)
	}

	runDir := filepath.Join(runtimeDataDir(), stageRunId)
	if err := os.MkdirAll(runDir, 0o700); err != nil {
		return "", "", fmt.Errorf("create runtime dir: %w", err)
	}
	paramsPath = filepath.Join(runDir, "params.json")
	inputsPath = filepath.Join(runDir, "inputs.json")

	if err := os.WriteFile(paramsPath, []byte(paramsJSON), 0o600); err != nil {
		return "", "", fmt.Errorf("write params file: %w", err)
	}

	inputsPresignedURL = strings.TrimSpace(inputsPresignedURL)
	if inputsPresignedURL == "" {
		if err := os.WriteFile(inputsPath, []byte("{}"), 0o600); err != nil {
			return "", "", fmt.Errorf("write empty inputs file: %w", err)
		}
		return paramsPath, inputsPath, nil
	}

	resp, err := http.Get(inputsPresignedURL)
	if err != nil {
		return "", "", fmt.Errorf("download inputs: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("download inputs: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("read inputs body: %w", err)
	}
	var inputsCheck map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(body, &inputsCheck); unmarshalErr != nil {
		return "", "", fmt.Errorf("inputs blob is not valid JSON object: %w", unmarshalErr)
	}
	if err := os.WriteFile(inputsPath, body, 0o600); err != nil {
		return "", "", fmt.Errorf("write inputs file: %w", err)
	}
	return paramsPath, inputsPath, nil
}

func removeStageRuntimeDir(stageRunId string) {
	runDir := filepath.Join(runtimeDataDir(), stageRunId)
	_ = os.RemoveAll(runDir)
}
