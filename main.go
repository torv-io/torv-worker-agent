package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "torv.io/worker-agent/proto"
)

func main() {
	orch := getEnv("ORCHESTRATOR_URL", "torv.io:50052")
	orchHTTP := getEnv("ORCHESTRATOR_HTTP_URL", "http://torv.io:3000")
	secret := getEnv("WORKER_SECRET", "")
	addr := getEnv("WORKER_ADDRESS", "pipe-worker-agent:50051")
	nodeImage := getEnv("NODE_WORKER_AGENT_IMAGE", "ghcr.io/torv-io/torv-node-worker-agent:main")

	conn, err := grpc.NewClient(orch, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	agentClient := pb.NewAgentServiceClient(conn)
	ctx := context.Background()

	stream, err := agentClient.Subscribe(ctx)
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	if err := stream.Send(&pb.AgentRequest{
		Type: pb.RequestType_REQUEST_TYPE_REGISTER,
		Body: &pb.AgentRequest_Register{Register: &pb.RegisterBody{Secret: secret, Address: addr}},
	}); err != nil {
		log.Fatalf("register send: %v", err)
	}

	r, err := stream.Recv()
	if err != nil {
		log.Fatalf("register recv: %v", err)
	}
	if r.GetRegister() == nil || !r.GetRegister().GetSuccess() {
		errMsg := "unknown error"
		if r.GetRegister() != nil {
			errMsg = r.GetRegister().GetError()
		}
		log.Fatalf("register: %s", errMsg)
	}
	workerID := r.GetRegister().GetWorkerId()
	log.Printf("registered: %s", workerID)

	if err := stream.Send(&pb.AgentRequest{
		Type: pb.RequestType_REQUEST_TYPE_SUBSCRIBE,
		Body: &pb.AgentRequest_Subscribe{Subscribe: &pb.SubscribeBody{WorkerId: workerID}},
	}); err != nil {
		log.Fatalf("subscribe send: %v", err)
	}

	var statusMu sync.Mutex
	var sendMu sync.Mutex
	workerStatus := "idle"

	sendReq := func(req *pb.AgentRequest) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(req)
	}

	go func() {
		tick := time.NewTicker(10 * time.Second)
		for range tick.C {
			statusMu.Lock()
			s := workerStatus
			statusMu.Unlock()
			hb := &pb.HeartbeatBody{WorkerId: workerID, Status: s}
			if err := sendReq(&pb.AgentRequest{
				Type: pb.RequestType_REQUEST_TYPE_HEARTBEAT,
				Body: &pb.AgentRequest_Heartbeat{Heartbeat: hb},
			}); err != nil {
				log.Printf("heartbeat: %v", err)
				return
			}
		}
	}()

	for {
		resp, err := stream.Recv()
		if err != nil {
			log.Fatalf("recv: %v", err)
		}
		if w := resp.GetWorkItem(); w != nil {
			log.Printf("work_item: stage_id=%s stage_run_id=%s", w.StageId, w.StageRunId)
			statusMu.Lock()
			workerStatus = "busy"
			statusMu.Unlock()

			go func(stageID, stageRunID string) {
				if err := runStage(ctx, stream, sendReq, workerID, stageRunID, orchHTTP, secret, nodeImage); err != nil {
					log.Printf("run_stage %s: %v", stageRunID, err)
				}
				statusMu.Lock()
				workerStatus = "idle"
				statusMu.Unlock()
			}(w.StageId, w.StageRunId)
		} else {
			log.Printf("message: %v", resp)
		}
	}
}

type sendFunc func(*pb.AgentRequest) error

func runStage(ctx context.Context, stream pb.AgentService_SubscribeClient, send sendFunc, workerID, stageRunID, orchHTTP, secret, nodeImage string) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", orchHTTP+"/api/agent/stage-run-payload/"+stageRunID, nil)
	req.Header.Set("X-Worker-Secret", secret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		sendFinish(send, workerID, stageRunID, 1, err.Error(), nil)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		sendFinish(send, workerID, stageRunID, 1, "payload fetch failed: "+resp.Status, nil)
		return nil
	}

	var payload struct {
		Code        string `json:"code"`
		Context     string `json:"context"`
		StageConfig string `json:"stageConfig"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		sendFinish(send, workerID, stageRunID, 1, "payload decode: "+err.Error(), nil)
		return err
	}

	send(&pb.AgentRequest{
		Type: pb.RequestType_REQUEST_TYPE_HANDLE_STAGE_START,
		Body: &pb.AgentRequest_HandleStageStart{HandleStageStart: &pb.HandleStageStartBody{WorkerId: workerID, StageRunId: stageRunID}},
	})

	exitCode, outputs, runErr := runNodeWorker(ctx, nodeImage, payload.Code, payload.Context, payload.StageConfig)

	var errMsg string
	if runErr != nil {
		errMsg = runErr.Error()
	}
	if exitCode != 0 && errMsg == "" {
		errMsg = "stage execution failed"
	}

	sendFinish(send, workerID, stageRunID, exitCode, errMsg, outputs)
	return runErr
}

func runNodeWorker(ctx context.Context, image, code, contextJSON, stageConfig string) (int, map[string]string, error) {
	payload := map[string]string{"code": code, "context": contextJSON, "stageConfig": stageConfig}
	payloadBytes, _ := json.Marshal(payload)

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return 1, nil, err
	}
	defer cli.Close()

	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image:       image,
		AttachStdin: true,
		AttachStdout: true,
		AttachStderr: true,
		OpenStdin:   true,
		StdinOnce:   true,
	}, &container.HostConfig{
		AutoRemove: true,
	}, nil, nil, "")
	if err != nil {
		return 1, nil, err
	}

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return 1, nil, err
	}

	attach, err := cli.ContainerAttach(ctx, resp.ID, container.AttachOptions{
		Stream: true,
		Stdin:  true,
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		return 1, nil, err
	}
	defer attach.Close()

	attach.Conn.Write(payloadBytes)
	attach.Conn.Close()

	var outBuf bytes.Buffer
	go stdcopy.StdCopy(&outBuf, io.Discard, attach.Reader)

	statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	var exitCode int
	select {
	case err := <-errCh:
		if err != nil {
			return 1, nil, err
		}
	case st := <-statusCh:
		exitCode = int(st.StatusCode)
	}

	outputs := make(map[string]string)
	scanner := bufio.NewScanner(&outBuf)
	for scanner.Scan() {
		var line struct {
			Type    string            `json:"type"`
			Success bool              `json:"success"`
			Outputs map[string]string `json:"outputs"`
			Error   string            `json:"error"`
		}
		if json.Unmarshal(scanner.Bytes(), &line) == nil && line.Type == "result" {
			if line.Outputs != nil {
				outputs = line.Outputs
			}
			break
		}
	}

	return exitCode, outputs, nil
}

func sendFinish(send sendFunc, workerID, stageRunID string, status int, errMsg string, outputs map[string]string) {
	out := outputs
	if out == nil {
		out = make(map[string]string)
	}
	send(&pb.AgentRequest{
		Type: pb.RequestType_REQUEST_TYPE_HANDLE_STAGE_FINISH,
		Body: &pb.AgentRequest_HandleStageFinish{
			HandleStageFinish: &pb.HandleStageFinishBody{
				StageRunId: stageRunID,
				Status:     int32(status),
				Error:      errMsg,
				Outputs:    out,
			},
		},
	})
}

func getEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

