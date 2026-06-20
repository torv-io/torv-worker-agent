package main

import (
	"context"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/docker/docker/client"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	agent "torv.io/worker-agent/proto"
)

var (
	workerId string
	status   = "idle"
	statusMu sync.Mutex
)

func setStatus(s string) {
	statusMu.Lock()
	status = s
	statusMu.Unlock()
}

func getStatus() string {
	statusMu.Lock()
	defer statusMu.Unlock()
	return status
}

func main() {
	verifyEnv()
	registrationMode := registrationModeLabel()

	conn, err := grpc.NewClient(os.Getenv("ORCHESTRATOR_URL"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to orchestrator: %v", err)
	}
	defer conn.Close()

	grpcClient := agent.NewAgentServiceClient(conn)

	stream, err := grpcClient.Subscribe(context.Background())
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	register := buildRegisterBody()
	stream.Send(&agent.AgentRequest{
		Type: agent.RequestType_REQUEST_TYPE_REGISTER,
		Body: &agent.AgentRequest_Register{
			Register: register,
		},
	})

	resp, err := stream.Recv()
	if err != nil {
		log.Fatalf("register recv: %v", err)
	}
	if r := resp.GetRegister(); r != nil && r.GetSuccess() {
		workerId = r.GetWorkerId()
	} else {
		log.Fatalf("registration failed: %v", resp.GetRegister().GetError())
	}
	log.Printf("registered as %s (%s)", workerId, registrationMode)
	log.Printf("orchestrator gRPC %s, HTTP %s", os.Getenv("ORCHESTRATOR_URL"), os.Getenv("ORCHESTRATOR_HTTP_URL"))

	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("docker client: %v", err)
	}
	defer dockerClient.Close()

	executor := &Executor{
		docker:   dockerClient,
		stream:   stream,
		workerId: workerId,
		images: map[string]string{
			"node":   envOrDefault("NODE_WORKER_AGENT_IMAGE", "ghcr.io/torv-io/torv-node-worker-agent:main"),
			"python": envOrDefault("PYTHON_WORKER_AGENT_IMAGE", "ghcr.io/torv-io/torv-python-worker-agent:main"),
		},
		networkName: resolveDockerNetwork(),
	}

	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				log.Printf("recv error: %v", err)
				os.Exit(1)
			}
			if resp.Type == agent.ResponseType_RESPONSE_TYPE_WORK_ITEM {
				wi := resp.GetWorkItem()
				log.Printf("work_item: stage_id=%s stage_run_id=%s runner=%s", wi.GetStageId(), wi.GetStageRunId(), wi.GetRunnerType())
				go executor.HandleWorkItem(wi)
			}
		}
	}()

	for {
		stream.Send(&agent.AgentRequest{
			Type: agent.RequestType_REQUEST_TYPE_HEARTBEAT,
			Body: &agent.AgentRequest_Heartbeat{
				Heartbeat: &agent.HeartbeatBody{
					WorkerId: workerId,
					Status:   getStatus(),
				},
			},
		})
		time.Sleep(10 * time.Second)
	}
}

// Registration modes (one binary):
//   - Self-hosted:     WORKER_SECRET + WORKSPACE_ID
//   - Dedicated cloud: BOOTSTRAP_TOKEN + WORKSPACE_ID
//   - Fleet:           BOOTSTRAP_TOKEN only (no workspace)
func verifyEnv() {
	godotenv.Load("../.env", ".env")

	for _, name := range []string{"ORCHESTRATOR_URL", "ORCHESTRATOR_HTTP_URL"} {
		if os.Getenv(name) == "" {
			log.Fatalf("missing env: %s", name)
		}
	}

	if os.Getenv("BOOTSTRAP_TOKEN") != "" {
		return
	}

	for _, name := range []string{"WORKER_SECRET", "WORKSPACE_ID"} {
		if os.Getenv(name) == "" {
			log.Fatalf("missing env: %s (or set BOOTSTRAP_TOKEN for cloud/fleet hosts)", name)
		}
	}
}

func registrationModeLabel() string {
	if os.Getenv("BOOTSTRAP_TOKEN") != "" {
		if os.Getenv("WORKSPACE_ID") != "" {
			return "dedicated cloud"
		}
		return "fleet"
	}
	return "self-hosted"
}

func buildRegisterBody() *agent.RegisterBody {
	body := &agent.RegisterBody{Address: ""}

	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		body.ReportedHostname = hostname
	}
	body.ReportedOs = runtime.GOOS
	if label := os.Getenv("HOST_LABEL"); label != "" {
		body.HostLabel = label
	}

	if bootstrap := os.Getenv("BOOTSTRAP_TOKEN"); bootstrap != "" {
		body.BootstrapToken = bootstrap
		if workspaceID := os.Getenv("WORKSPACE_ID"); workspaceID != "" {
			body.WorkspaceId = workspaceID
		}
		return body
	}

	body.Secret = os.Getenv("WORKER_SECRET")
	body.WorkspaceId = os.Getenv("WORKSPACE_ID")
	return body
}

func resolveDockerNetwork() string {
	if network := os.Getenv("DOCKER_NETWORK"); network != "" {
		return network
	}
	if os.Getenv("BOOTSTRAP_TOKEN") != "" {
		return "bridge"
	}
	return "pipe_torv_worker_network"
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
