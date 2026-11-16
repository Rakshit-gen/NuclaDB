package grpc

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Rakshit-gen/nucladb/internal/auth"
	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

func TestToStatusUsesSentinelsThroughWrapping(t *testing.T) {
	cases := map[error]codes.Code{
		engine.ErrTenantNotFound:  codes.NotFound,
		engine.ErrTenantExists:    codes.AlreadyExists,
		hnsw.ErrDimensionMismatch: codes.InvalidArgument,
		engine.ErrQuotaExceeded:   codes.ResourceExhausted,
		fmt.Errorf("disk full"):   codes.Internal,
		// A message that merely mentions "not found" is not a NotFound.
		fmt.Errorf("file not found"): codes.Internal,
	}
	for err, want := range cases {
		wrapped := fmt.Errorf("context: %w", err)
		if got := status.Code(toStatus(wrapped)); got != want {
			t.Errorf("%v: got %v, want %v", wrapped, got, want)
		}
	}
}

func TestKeyStoreStatusHidesSaveDetail(t *testing.T) {
	err := keyStoreStatus(fmt.Errorf("%w: %w", auth.ErrSave, errors.New("PUT bucket: 403 <Error>secret detail</Error>")), codes.InvalidArgument)
	if status.Code(err) != codes.Unavailable || strings.Contains(err.Error(), "secret detail") {
		t.Fatalf("got %v", err)
	}
	if err := keyStoreStatus(errors.New("auth: key name is over 64 bytes"), codes.InvalidArgument); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("validation error: got %v", err)
	}
}
