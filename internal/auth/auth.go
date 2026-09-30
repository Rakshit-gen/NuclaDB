// Package auth checks API keys. Each key is allowed a list of tenants ("*"
// for all of them) and may be an admin key, which is needed to create,
// delete and re-quota tenants.
package auth

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	ErrNoKey     = errors.New("auth: missing or unknown API key")
	ErrForbidden = errors.New("auth: API key not allowed here")
)

// Key is one entry in the keys file.
type Key struct {
	Key     string   `json:"key"`
	Tenants []string `json:"tenants"` // "*" allows every tenant
	Admin   bool     `json:"admin"`
}

// Keys is the loaded keys file.
type Keys struct{ keys []Key }

// Load reads a JSON array of Key from path.
func Load(path string) (*Keys, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var keys []Key
	if err := json.Unmarshal(b, &keys); err != nil {
		return nil, fmt.Errorf("auth: %s: %w", path, err)
	}
	for i, k := range keys {
		if len(k.Key) < 16 {
			return nil, fmt.Errorf("auth: %s: key %d is shorter than 16 characters", path, i)
		}
	}
	return &Keys{keys: keys}, nil
}

// Allow reports whether the key in ctx may use tenant, and admin calls if
// admin is set. An empty tenant is the default tenant.
func (k *Keys) Allow(ctx context.Context, tenant string, admin bool) error {
	given, _ := ctx.Value(ctxKey{}).(string)
	var found *Key
	for i := range k.keys {
		// Compare every key in constant time so timing doesn't leak how
		// much of one matched.
		if subtle.ConstantTimeCompare([]byte(k.keys[i].Key), []byte(given)) == 1 {
			found = &k.keys[i]
		}
	}
	if given == "" || found == nil {
		return ErrNoKey
	}
	if admin && !found.Admin {
		return ErrForbidden
	}
	if tenant == "" {
		tenant = "default"
	}
	if !admin && !slices.Contains(found.Tenants, "*") && !slices.Contains(found.Tenants, tenant) {
		return ErrForbidden
	}
	return nil
}

type ctxKey struct{}

// WithKey returns ctx carrying the caller's API key.
func WithKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, ctxKey{}, key)
}

// FromHeader takes the key from an "Authorization: Bearer <key>" or
// "X-API-Key: <key>" header value pair.
func FromHeader(authorization, apiKey string) string {
	if k, ok := strings.CutPrefix(authorization, "Bearer "); ok {
		return k
	}
	return apiKey
}

// UnaryServerInterceptor moves the key from gRPC metadata into the context.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		first := func(name string) string {
			if v := md.Get(name); len(v) > 0 {
				return v[0]
			}
			return ""
		}
		return handler(WithKey(ctx, FromHeader(first("authorization"), first("x-api-key"))), req)
	}
}

// Bearer sends an API key on every RPC, for server-to-server clients
// (grpc.WithPerRPCCredentials(auth.Bearer(key))).
type Bearer string

func (b Bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}

// RequireTransportSecurity is false so a key also works on a plaintext
// localhost link; over a network, pair it with TLS.
func (Bearer) RequireTransportSecurity() bool { return false }
