package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

// Orchestrator sends this instead of the file bytes. The files are already under TORV_DATA_ROOT.
const artifactInputsPrefix = "artifacts:"

var artifactPathPattern = regexp.MustCompile(`^runs/[A-Za-z0-9_-]+/out/[A-Za-z0-9_.-]+\.json$`)

type artifactRef struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

func isArtifactInputs(inputsURL string) bool {
	return strings.HasPrefix(inputsURL, artifactInputsPrefix)
}

// Reads upstream output files off local disk and copies them into the stage container.
func copyArtifactInputs(ctx context.Context, docker *client.Client, containerID, dataRoot, workspaceID, inputsURL string) error {
	if !isArtifactInputs(inputsURL) {
		return nil
	}
	var refs []artifactRef
	if err := json.Unmarshal([]byte(strings.TrimPrefix(inputsURL, artifactInputsPrefix)), &refs); err != nil {
		return fmt.Errorf("artifact inputs: %w", err)
	}
	if len(refs) == 0 {
		return nil
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "torv-inputs/", Mode: 0o755, Typeflag: tar.TypeDir}); err != nil {
		return err
	}
	for _, ref := range refs {
		if err := validateOutputName(ref.Name); err != nil {
			return err
		}
		rel, err := safeArtifactPath(ref.Path)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(dataRoot, workspaceID, rel))
		if err != nil {
			return fmt.Errorf("read artifact %s: %w", ref.Name, err)
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: "torv-inputs/" + ref.Name + ".json",
			Mode: 0o644,
			Size: int64(len(body)),
		}); err != nil {
			return err
		}
		if _, err := tw.Write(body); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return docker.CopyToContainer(ctx, containerID, "/", &buf, types.CopyToContainerOptions{})
}

func safeArtifactPath(p string) (string, error) {
	if filepath.Clean(p) != p || !artifactPathPattern.MatchString(p) {
		return "", fmt.Errorf("invalid artifact path %q", p)
	}
	return p, nil
}
