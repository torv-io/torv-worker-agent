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
	pb "torv.io/worker-agent/proto"
)

type Executor struct {
	docker        *client.Client
	stream        pb.WorkerService_SubscribeClient
	workerID      string
	image         string
	network       string
	dataRoot      string
	workspaceID   string
	computeHostID string
}

type stdoutLine struct {
	Type    string          `json:"type"`
	Level   string          `json:"level,omitempty"`
	Message string          `json:"message,omitempty"`
	Success bool            `json:"success,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
	Outputs json.RawMessage `json:"outputs,omitempty"`
}

type streamCaptured struct {
	resultSeen    bool
	resultSuccess bool
	resultError   string
	resultOutputs string
}

func (e *Executor) HandleDispatch(dispatch *pb.RunDispatch) {
	runID := dispatch.GetRunId()
	setStatus("busy")
	defer setStatus("idle")

	if strings.ToLower(dispatch.GetRunnerType()) != "node" {
		e.sendResult(runID, "failed", 1, fmt.Sprintf("unsupported runner_type: %s", dispatch.GetRunnerType()), "")
		return
	}
	if dispatch.GetCodeUrl() == "" || dispatch.GetConfigUrl() == "" {
		e.sendResult(runID, "failed", 1, "missing code_url or config_url", "")
		return
	}
	if err := validateParamsJSON(dispatch.GetParamsJson()); err != nil {
		e.sendResult(runID, "failed", 1, err.Error(), "")
		return
	}

	exitCode, captured, err := e.runContainer(dispatch)
	if err != nil {
		e.sendResult(runID, "failed", 1, err.Error(), "")
		return
	}
	if captured.resultSeen {
		status := "failed"
		outputsJSON := ""
		if captured.resultSuccess {
			status = "EXECUTED"
			manifest, err := writeOutputArtifacts(e.dataRoot, e.workspaceID, runID, e.computeHostID, captured.resultOutputs)
			if err != nil {
				e.sendResult(runID, "failed", int32(exitCode), err.Error(), "")
				return
			}
			outputsJSON = manifest
		}
		e.sendResult(runID, status, int32(exitCode), captured.resultError, outputsJSON)
		return
	}
	reason := ""
	if exitCode != 0 {
		reason = fmt.Sprintf("container exited with status %d", exitCode)
	}
	e.sendResult(runID, "failed", int32(exitCode), reason, "")
}

func (e *Executor) runContainer(dispatch *pb.RunDispatch) (int, streamCaptured, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	runID := dispatch.GetRunId()
	var captured streamCaptured

	env := []string{
		"CODE_URL=" + dispatch.GetCodeUrl(),
		"CONFIG_URL=" + dispatch.GetConfigUrl(),
		"TORV_PARAMS_JSON=" + dispatch.GetParamsJson(),
		"INPUTS_URL=" + dispatch.GetInputsUrl(),
		"RUN_ID=" + runID,
		"STAGE_ID=" + dispatch.GetStageId(),
	}

	containerCfg := &container.Config{
		Image:        e.image,
		WorkingDir:   "/app",
		Env:          env,
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          []string{"/bin/sh", "/app/bootstrap.sh"},
	}

	hostCfg := &container.HostConfig{}
	var networkCfg *network.NetworkingConfig
	if e.network == "bridge" {
		hostCfg.NetworkMode = "bridge"
	} else {
		networkCfg = &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{e.network: {}},
		}
	}

	created, err := e.docker.ContainerCreate(ctx, containerCfg, hostCfg, networkCfg, nil, "run-"+runID)
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

	captured = e.streamOutput(runID, attach.Reader)

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

func (e *Executor) streamOutput(runID string, reader io.Reader) streamCaptured {
	var captured streamCaptured

	pr, pw := io.Pipe()
	go func() {
		stdcopy.StdCopy(pw, pw, reader)
		pw.Close()
	}()

	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var parsed stdoutLine
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			e.sendLog(runID, "info", line)
			continue
		}

		switch parsed.Type {
		case "log":
			e.sendLog(runID, parsed.Level, parsed.Message)
		case "result":
			captured.resultSeen = true
			captured.resultSuccess = parsed.Success
			if len(parsed.Outputs) > 0 && string(parsed.Outputs) != "null" {
				captured.resultOutputs = string(parsed.Outputs)
			}
			if !parsed.Success {
				captured.resultError = strings.TrimSpace(string(parsed.Error))
			}
		}
	}

	return captured
}

func (e *Executor) sendLog(runID, level, message string) {
	log.Printf("[%s] [%s] %s", runID, level, message)
	_ = e.stream.Send(&pb.WorkerMessage{
		WorkerId: e.workerID,
		Type: &pb.WorkerMessage_Log{
			Log: &pb.RunLog{RunId: runID, Level: level, Message: message},
		},
	})
}

func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (e *Executor) sendResult(runID, status string, code int32, errMsg, outputsJSON string) {
	log.Printf("[%s] result status=%s code=%d error=%q", runID, status, code, errMsg)
	_ = e.stream.Send(&pb.WorkerMessage{
		WorkerId: e.workerID,
		Type: &pb.WorkerMessage_Result{
			Result: &pb.RunResult{
				RunId:       runID,
				Status:      status,
				Code:        code,
				Error:       optStr(errMsg),
				OutputsJson: optStr(outputsJSON),
			},
		},
	})
}
