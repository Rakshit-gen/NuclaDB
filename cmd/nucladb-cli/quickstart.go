package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// runQuickstart spins up a throwaway nucladbd against a temp data dir on
// free local ports, runs a scripted insert+search demo through the same
// run* functions the real subcommands use, then tears it all down. Never
// touches a real/already-running server.
func runQuickstart(args []string) error {
	nucladbd, err := findNucladbd()
	if err != nil {
		return err
	}

	dataDir, err := os.MkdirTemp("", "nucladb-quickstart-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dataDir) }()

	grpcAddr, err := freeAddr()
	if err != nil {
		return err
	}
	httpAddr, err := freeAddr()
	if err != nil {
		return err
	}

	fmt.Printf("Starting a throwaway nucladbd (dim=4, metric=l2) at %s ...\n", grpcAddr)
	cmd := exec.Command(nucladbd,
		"-data-dir="+dataDir,
		"-grpc-addr="+grpcAddr,
		"-http-addr="+httpAddr,
		"-dim=4",
		"-metric=l2",
	)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting nucladbd: %w", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	if err := waitForServing(grpcAddr, 10*time.Second); err != nil {
		return fmt.Errorf("nucladbd never became ready: %w", err)
	}
	fmt.Println("Server is up. Running a few commands against it:")

	fmt.Println("\n$ nucladb-cli insert -id=1 -vector=1,0,0,0 -meta=team=search")
	if err := runInsert(grpcAddr, []string{"-id=1", "-vector=1,0,0,0", "-meta=team=search"}); err != nil {
		return err
	}

	fmt.Println("\n$ nucladb-cli insert -id=2 -vector=0,1,0,0 -meta=team=infra")
	if err := runInsert(grpcAddr, []string{"-id=2", "-vector=0,1,0,0", "-meta=team=infra"}); err != nil {
		return err
	}

	fmt.Println("\n$ nucladb-cli search -vector=1,0,0,0 -top-k=2")
	if err := runSearch(grpcAddr, []string{"-vector=1,0,0,0", "-top-k=2"}); err != nil {
		return err
	}

	fmt.Println("\nThat's the whole loop: insert, search, delete, batch-upsert all work the")
	fmt.Println("same way against a real nucladbd. Shutting the throwaway server down now.")
	fmt.Println("Run \"nucladb-cli <command> -h\" for flags, or see docs/cli.md.")
	return nil
}

// findNucladbd looks next to the running nucladb-cli binary first (a
// dev checkout with both built side by side), then falls back to $PATH.
func findNucladbd() (string, error) {
	if self, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(self), "nucladbd")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	if path, err := exec.LookPath("nucladbd"); err == nil {
		return path, nil
	}
	return "", fmt.Errorf(
		"quickstart: can't find the nucladbd server binary.\n" +
			"Install it with: go install github.com/Rakshit-gen/nucladb/cmd/nucladbd@latest",
	)
}

func freeAddr() (string, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().String(), nil
}

func waitForServing(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := dial(addr)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
			cancel()
			_ = conn.Close()
			if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %s", timeout)
}
