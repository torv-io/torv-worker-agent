# Build stage
FROM golang:1.24-alpine AS builder

# git/ca-certificates are needed for Go module downloads. The protobuf code is committed
# under proto/ (agent.pb.go, agent_grpc.pb.go), so we do not install protoc or regenerate it.
RUN apk add --no-cache git ca-certificates

WORKDIR /build

# Download dependencies first for better layer caching.
COPY go.mod go.sum ./
RUN go mod download

# Copy committed proto bindings and source.
COPY proto/ ./proto/
COPY *.go ./

# Build the binary.
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -o worker-agent .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /build/worker-agent .

# Expose gRPC port
EXPOSE 50051

# Run the binary
CMD ["./worker-agent"]
