package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

func validateParamsJSON(paramsJSON string) error {
	paramsJSON = strings.TrimSpace(paramsJSON)
	if paramsJSON == "" {
		return fmt.Errorf("params_json missing on work item")
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal([]byte(paramsJSON), &params); err != nil {
		return fmt.Errorf("params_json is not valid JSON object: %w", err)
	}
	return nil
}
