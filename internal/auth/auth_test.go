package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestAllow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys.json")
	os.WriteFile(path, []byte(`[
		{"key": "admin-key-0123456789", "tenants": ["*"], "admin": true},
		{"key": "acme-key-0123456789", "tenants": ["acme"]},
		{"key": "all-key-01234567890", "tenants": ["*"]}
	]`), 0o600)
	k, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := func(key string) context.Context { return WithKey(context.Background(), key) }
	cases := []struct {
		key, tenant string
		admin       bool
		want        error
	}{
		{"", "acme", false, ErrNoKey},
		{"wrong-key-0123456789", "acme", false, ErrNoKey},
		{"acme-key-0123456789", "acme", false, nil},
		{"acme-key-0123456789", "globex", false, ErrForbidden},
		{"acme-key-0123456789", "", false, ErrForbidden}, // default tenant not listed
		{"acme-key-0123456789", "acme", true, ErrForbidden},
		{"all-key-01234567890", "globex", false, nil},
		{"all-key-01234567890", "", true, ErrForbidden},
		{"admin-key-0123456789", "", true, nil},
	}
	for _, c := range cases {
		if got := k.Allow(ctx(c.key), c.tenant, c.admin); got != c.want {
			t.Errorf("key %q tenant %q admin %v: got %v, want %v", c.key, c.tenant, c.admin, got, c.want)
		}
	}

	os.WriteFile(path, []byte(`[{"key": "short", "tenants": ["*"]}]`), 0o600)
	if _, err := Load(path); err == nil {
		t.Fatal("a short key was accepted")
	}
}
