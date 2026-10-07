package workflowprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"solo-0014-zone-rollout-control/internal/allocation"
	"solo-0014-zone-rollout-control/internal/assessment"
	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/fleet"
	"solo-0014-zone-rollout-control/internal/httpapi"
	"solo-0014-zone-rollout-control/internal/plan"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/probe"
	"solo-0014-zone-rollout-control/internal/store"
)

type harness struct {
	baseURL  string
	clock    *platform.ManualClock
	client   *http.Client
	server   *http.Server
	listener net.Listener
	tempDir  string
}

func newHarness() (*harness, error) {
	tempDir, err := os.MkdirTemp("", "zrc-probe-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary directory: %w", err)
	}
	h := &harness{
		clock:   platform.NewManualClock(time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)),
		client:  &http.Client{Timeout: 10 * time.Second},
		tempDir: tempDir,
	}
	if err := h.serve(); err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, err
	}
	return h, nil
}

// serve opens the snapshot store and starts the HTTP server on a random port.
func (h *harness) serve() error {
	stateStore, err := store.Open(h.snapshotPath())
	if err != nil {
		return fmt.Errorf("open temporary store: %w", err)
	}
	ids := platform.CryptoIDGenerator{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	plans := plan.New(stateStore, h.clock, ids)
	nodes := fleet.New(stateStore, h.clock, ids)
	allocations := allocation.New(stateStore, h.clock, ids)
	probes := probe.New(stateStore, h.clock, ids, allocations)
	assessments := assessment.New(stateStore, h.clock, ids)
	api := httpapi.New(plans, nodes, allocations, probes, assessments, ids, logger)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	server := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = server.Serve(listener)
	}()
	h.baseURL = "http://" + listener.Addr().String()
	h.server = server
	h.listener = listener
	return nil
}

func (h *harness) snapshotPath() string {
	return h.tempDir + "/state.json"
}

// restart shuts the server down, lets the caller rewrite the snapshot file
// directly (bypassing store reconciliation, to emulate legacy snapshots), and
// boots a fresh server from the modified snapshot.
func (h *harness) restart(mutate func(state *domain.State) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := h.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown server: %w", err)
	}
	_ = h.listener.Close()
	if mutate != nil {
		payload, err := os.ReadFile(h.snapshotPath())
		if err != nil {
			return fmt.Errorf("read snapshot: %w", err)
		}
		var state domain.State
		if err := json.Unmarshal(payload, &state); err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}
		if err := mutate(&state); err != nil {
			return err
		}
		updated, err := json.Marshal(state)
		if err != nil {
			return fmt.Errorf("encode snapshot: %w", err)
		}
		if err := os.WriteFile(h.snapshotPath(), updated, 0o600); err != nil {
			return fmt.Errorf("write snapshot: %w", err)
		}
	}
	return h.serve()
}

func (h *harness) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = h.server.Shutdown(ctx)
	_ = h.listener.Close()
	_ = os.RemoveAll(h.tempDir)
}

func (h *harness) call(method string, path string, requestBody any, target any) error {
	var payload []byte
	var err error
	if requestBody != nil {
		payload, err = json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}
	request, err := http.NewRequest(method, h.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		return fmt.Errorf("perform request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &failure); err != nil {
			return fmt.Errorf("decode error response: %w", err)
		}
		return &apiFailure{
			Status:  response.StatusCode,
			Code:    failure.Error.Code,
			Message: failure.Error.Message,
		}
	}
	if target == nil {
		return nil
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if err := json.Unmarshal(envelope.Data, target); err != nil {
		return fmt.Errorf("decode response data: %w", err)
	}
	return nil
}
