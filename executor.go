package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	agent "torv.io/worker-agent/proto"
)

type Executor struct {
	docker      *client.Client
	stream      agent.AgentService_SubscribeClient
	workerId    string
	images      map[string]string
	networkName string
}

type stdoutLine struct {
	Type    string          `json:"type"`
	Level   string          `json:"level,omitempty"`
	Message string          `json:"message,omitempty"`
	Args    json.RawMessage `json:"args,omitempty"`
	Success bool            `json:"success,omitempty"`
	// Error may be a string or an array (e.g. errorResult(["..."])). Decoding into a fixed type
	// would fail the line and drop the whole result event — silently flipping failure to success.
	Error   json.RawMessage `json:"error,omitempty"`
	Outputs json.RawMessage `json:"outputs,omitempty"`
}

// extractErrorMessage coerces a result event's `error` field (a string or any JSON array) into a
// single human-readable message.
func extractErrorMessage(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return ""
	}
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		return strings.TrimSpace(str)
	}
	var arr []any
	if err := json.Unmarshal(raw, &arr); err == nil {
		parts := make([]string, 0, len(arr))
		for _, v := range arr {
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		return strings.TrimSpace(strings.Join(parts, "; "))
	}
	return s
}

// formattedLogContent joins message + serialized args from pipe-node-worker-agent.
// Without this, diagnostics passed as logger.error('label', { url, bodyPreview }) were dropped.
func formattedLogContent(parsed stdoutLine) string {
	msg := strings.TrimSpace(parsed.Message)
	args := strings.TrimSpace(string(parsed.Args))
	if args == "" || args == "null" {
		return msg
	}
	if msg == "" {
		return args
	}
	return msg + " " + args
}

type streamCaptured struct {
	resultSeen    bool
	resultSuccess bool
	resultError   string
	resultOutputs string
	lastErrorLog  string
}

const maxLogMessageBytes = 64 * 1024
const maxScanBufferBytes = 64 * 1024 * 1024

func truncateForLog(s string) string {
	if len(s) <= maxLogMessageBytes {
		return s
	}
	return s[:maxLogMessageBytes] + fmt.Sprintf("… (truncated %d bytes)", len(s)-maxLogMessageBytes)
}

func (e *Executor) HandleWorkItem(wi *agent.WorkItemBody) {
	setStatus("busy")
	defer setStatus("idle")

	stageRunId := wi.GetStageRunId()
	stageId := wi.GetStageId()

	e.sendStageStart(stageRunId)

	image, ok := e.images[strings.ToLower(wi.GetRunnerType())]
	if !ok {
		e.sendLog(stageRunId, "error", fmt.Sprintf("No image configured for runner type: %s", wi.GetRunnerType()))
		e.sendStageFinish(stageRunId, 1, fmt.Sprintf("Unknown runner type: %s", wi.GetRunnerType()), "")
		return
	}

	if wi.GetCodePresignedUrl() == "" || wi.GetConfigPresignedUrl() == "" {
		e.sendLog(stageRunId, "error", "Work item missing presigned URLs")
		e.sendStageFinish(stageRunId, 1, "Missing presigned URLs on work item", "")
		return
	}

	paramsJSON := wi.GetParamsJson()
	if err := validateParamsJSON(paramsJSON); err != nil {
		e.sendLog(stageRunId, "error", err.Error())
		e.sendStageFinish(stageRunId, 1, err.Error(), "")
		return
	}

	exitCode, captured, err := e.runContainer(stageRunId, stageId, image, wi, paramsJSON)
	if err != nil {
		e.sendLog(stageRunId, "error", fmt.Sprintf("Container execution failed: %v", err))
		e.sendStageFinish(stageRunId, 1, err.Error(), "")
		return
	}

	if captured.resultSeen {
		if captured.resultSuccess {
			e.sendStageFinish(stageRunId, 0, "", captured.resultOutputs)
			return
		}
		reason := captured.resultError
		if reason == "" {
			reason = "Stage reported failure"
		}
		e.sendStageFinish(stageRunId, 1, reason, captured.resultOutputs)
		return
	}

	reason := ""
	if exitCode != 0 {
		if captured.lastErrorLog != "" {
			reason = captured.lastErrorLog
		} else {
			reason = fmt.Sprintf("Stage exited with non-zero status %d before reporting a result", exitCode)
		}
	}
	e.sendStageFinish(stageRunId, int32(exitCode), reason, "")
}

func (e *Executor) runContainer(stageRunId, stageId, image string, wi *agent.WorkItemBody, paramsJSON string) (int, streamCaptured, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	containerName := fmt.Sprintf("stage-run-%s", stageRunId)

	env := []string{
		"CODE_PRESIGNED_URL=" + wi.GetCodePresignedUrl(),
		"CONFIG_PRESIGNED_URL=" + wi.GetConfigPresignedUrl(),
		"TORV_PARAMS_JSON=" + paramsJSON,
		"INPUTS_PRESIGNED_URL=" + wi.GetInputsPresignedUrl(),
		"STAGE_ID=" + stageId,
		"STAGE_RUN_ID=" + stageRunId,
	}

	containerCfg := &container.Config{
		Image:        image,
		WorkingDir:   "/app",
		Env:          env,
		OpenStdin:    false,
		AttachStdin:  false,
		AttachStdout: true,
		AttachStderr: true,
		Cmd: []string{"/bin/sh", "/app/bootstrap.sh"},
	}

	hostCfg := &container.HostConfig{}

	networkCfg := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			e.networkName: {},
		},
	}

	var captured streamCaptured

	created, err := e.docker.ContainerCreate(ctx, containerCfg, hostCfg, networkCfg, nil, containerName)
	if err != nil {
		return -1, captured, fmt.Errorf("create container: %w", err)
	}
	containerID := created.ID

	defer func() {
		rmCtx, rmCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer rmCancel()
		e.docker.ContainerRemove(rmCtx, containerID, container.RemoveOptions{Force: true})
	}()

	attach, err := e.docker.ContainerAttach(ctx, containerID, container.AttachOptions{
		Stream: true,
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		return -1, captured, fmt.Errorf("attach: %w", err)
	}
	defer attach.Close()

	if err := e.docker.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return -1, captured, fmt.Errorf("start: %w", err)
	}

	captured = e.streamOutput(stageRunId, attach.Reader)

	waitCh, errCh := e.docker.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case result := <-waitCh:
		return int(result.StatusCode), captured, nil
	case err := <-errCh:
		return -1, captured, fmt.Errorf("wait: %w", err)
	case <-ctx.Done():
		return -1, captured, fmt.Errorf("container timed out")
	}
}

func (e *Executor) streamOutput(stageRunId string, reader io.Reader) streamCaptured {
	var captured streamCaptured

	pr, pw := io.Pipe()
	go func() {
		stdcopy.StdCopy(pw, pw, reader)
		pw.Close()
	}()

	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 0, 64*1024), maxScanBufferBytes)
	defer func() {
		if err := scanner.Err(); err != nil {
			e.sendLog(stageRunId, "error", fmt.Sprintf("worker scanner aborted: %v — subsequent stage output (including the result event) was dropped", err))
		}
	}()
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var parsed stdoutLine
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			e.sendLog(stageRunId, "info", truncateForLog(line))
			continue
		}

		switch parsed.Type {
		case "log":
			content := formattedLogContent(parsed)
			e.sendLog(stageRunId, parsed.Level, truncateForLog(content))
			if strings.EqualFold(parsed.Level, "error") && content != "" {
				captured.lastErrorLog = content
			}
		case "result":
			captured.resultSeen = true
			if len(parsed.Outputs) > 0 && string(parsed.Outputs) != "null" {
				captured.resultOutputs = string(parsed.Outputs)
			}
			if parsed.Success {
				captured.resultSuccess = true
				e.sendLog(stageRunId, "info", "Stage completed successfully")
			} else {
				captured.resultSuccess = false
				if msg := extractErrorMessage(parsed.Error); msg != "" {
					captured.resultError = msg
					e.sendLog(stageRunId, "error", truncateForLog(msg))
				} else {
					e.sendLog(stageRunId, "error", "Stage reported failure (no message)")
				}
			}
		default:
			e.sendLog(stageRunId, "info", truncateForLog(line))
		}
	}

	return captured
}

func (e *Executor) sendStageStart(stageRunId string) {
	log.Printf("[%s] stage start", stageRunId)
	e.stream.Send(&agent.AgentRequest{
		Type: agent.RequestType_REQUEST_TYPE_HANDLE_STAGE_START,
		Body: &agent.AgentRequest_HandleStageStart{
			HandleStageStart: &agent.HandleStageStartBody{
				WorkerId:   e.workerId,
				StageRunId: stageRunId,
				Message:    "Stage execution started",
			},
		},
	})
}

func (e *Executor) sendStageFinish(stageRunId string, exitCode int32, errorMsg, outputsJson string) {
	log.Printf("[%s] stage finish (exit=%d, error=%q, outputs=%dB)", stageRunId, exitCode, errorMsg, len(outputsJson))
	e.stream.Send(&agent.AgentRequest{
		Type: agent.RequestType_REQUEST_TYPE_HANDLE_STAGE_FINISH,
		Body: &agent.AgentRequest_HandleStageFinish{
			HandleStageFinish: &agent.HandleStageFinishBody{
				StageRunId:  stageRunId,
				Status:      exitCode,
				Error:       errorMsg,
				OutputsJson: outputsJson,
			},
		},
	})
}

func (e *Executor) sendLog(stageRunId, level, message string) {
	log.Printf("[%s] [%s] %s", stageRunId, level, message)
	e.stream.Send(&agent.AgentRequest{
		Type: agent.RequestType_REQUEST_TYPE_STAGE_RUN_MESSAGE,
		Body: &agent.AgentRequest_StageRunMessage{
			StageRunMessage: &agent.StageRunMessageBody{
				WorkerId:   e.workerId,
				StageRunId: stageRunId,
				Level:      level,
				Message:    message,
			},
		},
	})
}
