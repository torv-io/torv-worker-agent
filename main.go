package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/docker/docker/client"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "torv.io/worker-agent/proto"
)

var (
	workerID string
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
	godotenv.Load("../.env", ".env")
	for _, name := range []string{"ORCHESTRATOR_URL", "WORKER_SECRET", "WORKSPACE_ID"} {
		if os.Getenv(name) == "" {
			log.Fatalf("missing env: %s", name)
		}
	}

	conn, err := grpc.NewClient(os.Getenv("ORCHESTRATOR_URL"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	stream, err := pb.NewWorkerServiceClient(conn).Subscribe(context.Background())
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	if err := stream.Send(&pb.WorkerMessage{
		Body: &pb.WorkerMessage_Session{
			Session: &pb.Session{
				WorkspaceId: os.Getenv("WORKSPACE_ID"),
				Token:       os.Getenv("WORKER_SECRET"),
			},
		},
	}); err != nil {
		log.Fatalf("session send: %v", err)
	}

	ready, err := stream.Recv()
	if err != nil {
		log.Fatalf("session recv: %v", err)
	}
	session := ready.GetSession()
	if session == nil || session.GetError() != "" {
		log.Fatalf("session failed: %s", session.GetError())
	}
	workerID = session.GetWorkerId()
	log.Printf("connected as worker %s", workerID)

	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("docker: %v", err)
	}
	defer dockerClient.Close()

	executor := &Executor{
		docker:   dockerClient,
		stream:   stream,
		workerID: workerID,
		image:    envOrDefault("NODE_WORKER_AGENT_IMAGE", "ghcr.io/torv-io/torv-node-worker-agent:main"),
		network:  envOrDefault("DOCKER_NETWORK", "torv_worker_network"),
	}

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				log.Fatalf("recv: %v", err)
			}
			if dispatch := msg.GetDispatch(); dispatch != nil {
				log.Printf("dispatch run_id=%s stage_id=%s runner_id=%s runner_type=%s", dispatch.GetRunId(), dispatch.GetStageId(), dispatch.GetRunnerId(), dispatch.GetRunnerType())
				go executor.HandleDispatch(dispatch)
			}
			if abort := msg.GetAbort(); abort != nil {
				log.Printf("abort run_id=%s reason=%s", abort.GetRunId(), abort.GetReason())
				setStatus("idle")
			}
		}
	}()

	for {
		if err := stream.Send(&pb.WorkerMessage{
			WorkerId: workerID,
			Body: &pb.WorkerMessage_Heartbeat{
				Heartbeat: &pb.Heartbeat{Status: getStatus()},
			},
		}); err != nil {
			log.Fatalf("heartbeat send: %v", err)
		}
		time.Sleep(10 * time.Second)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
