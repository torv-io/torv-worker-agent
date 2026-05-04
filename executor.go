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
	Type    string `json:"type"`
	Level   string `json:"level,omitempty"`
	Message string `json:"message,omitempty"`
	Success bool   `json:"success,omitempty"`
	Error   string `json:"error,omitempty"`
}

// Captured information from the stage's stdout stream. The structured `result`
// event is authoritative for success/failure; container exit code is only used
// as a fallback when no result was emitted (e.g. the stage crashed before
// writing one).
type streamCaptured struct {
	resultSeen    bool
	resultSuccess bool
	resultError   string
	lastErrorLog  string
}

// Per-message and per-line caps to avoid forwarding pathologically large
// payloads (e.g. a stage that dumps a multi-MB array to a log message). 16 KB
// is plenty for any human-readable log line.
const maxLogMessageBytes = 16 * 1024

// Per-line cap for the raw stdout scanner. Stages that emit very large result
// payloads need headroom; 16 MB is far above anything reasonable but bounded
// enough to prevent runaway memory.
const maxScanBufferBytes = 16 * 1024 * 1024

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
		e.sendStageFinish(stageRunId, 1, fmt.Sprintf("Unknown runner type: %s", wi.GetRunnerType()))
		return
	}

	if wi.GetCodePresignedUrl() == "" || wi.GetConfigPresignedUrl() == "" {
		e.sendLog(stageRunId, "error", "Work item missing presigned URLs")
		e.sendStageFinish(stageRunId, 1, "Missing presigned URLs on work item")
		return
	}

	exitCode, captured, err := e.runContainer(stageRunId, stageId, image, wi)
	if err != nil {
		e.sendLog(stageRunId, "error", fmt.Sprintf("Container execution failed: %v", err))
		e.sendStageFinish(stageRunId, 1, err.Error())
		return
	}

	// The stage's structured `result` event is the source of truth. A non-zero
	// container exit code *after* a successful result (e.g. node process killed
	// by the kernel mid-flush of a huge stdout payload) must not be reported as
	// a failure — the stage's own outcome wins.
	if captured.resultSeen {
		if captured.resultSuccess {
			e.sendStageFinish(stageRunId, 0, "")
			return
		}
		reason := captured.resultError
		if reason == "" {
			reason = "Stage reported failure"
		}
		e.sendStageFinish(stageRunId, 1, reason)
		return
	}

	// No result event reached us — stage likely crashed before emitting one.
	// Fall back to exit code + best-effort reason from logs.
	reason := ""
	if exitCode != 0 {
		if captured.lastErrorLog != "" {
			reason = captured.lastErrorLog
		} else {
			reason = fmt.Sprintf("Stage exited with non-zero status %d before reporting a result", exitCode)
		}
	}
	e.sendStageFinish(stageRunId, int32(exitCode), reason)
}

func (e *Executor) runContainer(stageRunId, stageId, image string, wi *agent.WorkItemBody) (int, streamCaptured, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	containerName := fmt.Sprintf("stage-run-%s", stageRunId)

	env := []string{
		"CODE_PRESIGNED_URL=" + wi.GetCodePresignedUrl(),
		"CONFIG_PRESIGNED_URL=" + wi.GetConfigPresignedUrl(),
		"CONTEXT_JSON=" + wi.GetContextJson(),
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
		// Absolute path + sh: avoids missing WORKDIR from image metadata and Alpine (no bash in shebang).
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
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var parsed stdoutLine
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			// Non-JSON stdout: forward as info, but cap to keep huge dumps out of the console.
			e.sendLog(stageRunId, "info", truncateForLog(line))
			continue
		}

		switch parsed.Type {
		case "log":
			e.sendLog(stageRunId, parsed.Level, truncateForLog(parsed.Message))
			if strings.EqualFold(parsed.Level, "error") && parsed.Message != "" {
				captured.lastErrorLog = parsed.Message
			}
		case "result":
			// Structured result event — record the outcome and emit a clean,
			// human-readable line. Never echo the raw JSON: it can be huge and
			// is for the orchestrator/SDK, not the console reader.
			captured.resultSeen = true
			if parsed.Success {
				captured.resultSuccess = true
				e.sendLog(stageRunId, "info", "Stage completed successfully")
			} else {
				captured.resultSuccess = false
				if parsed.Error != "" {
					captured.resultError = parsed.Error
					e.sendLog(stageRunId, "error", truncateForLog(parsed.Error))
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

func (e *Executor) sendStageFinish(stageRunId string, exitCode int32, errorMsg string) {
	log.Printf("[%s] stage finish (exit=%d, error=%q)", stageRunId, exitCode, errorMsg)
	e.stream.Send(&agent.AgentRequest{
		Type: agent.RequestType_REQUEST_TYPE_HANDLE_STAGE_FINISH,
		Body: &agent.AgentRequest_HandleStageFinish{
			HandleStageFinish: &agent.HandleStageFinishBody{
				StageRunId: stageRunId,
				Status:     exitCode,
				Error:      errorMsg,
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
