package auth

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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

func TestIssuedKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api_keys.json")
	k, err := New([]Key{{Key: "admin-key-0123456789", Tenants: []string{"*"}, Admin: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := k.Issue("x", []string{"acme"}, false); err == nil {
		t.Fatal("Issue worked before IssuedFrom")
	}
	if err := k.IssuedFrom(path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := k.Issue("x", nil, false); err == nil {
		t.Fatal("issued a non-admin key with no tenants")
	}
	acme, is, err := k.Issue("ci", []string{"acme"}, false)
	if err != nil {
		t.Fatal(err)
	}
	admin, _, err := k.Issue("ops", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx := func(key string) context.Context { return WithKey(context.Background(), key) }
	allow := func(k *Keys, key, tenant string, admin bool, want error) {
		t.Helper()
		if got := k.Allow(ctx(key), tenant, admin); got != want {
			t.Errorf("key %q tenant %q admin %v: got %v, want %v", key, tenant, admin, got, want)
		}
	}
	allow(k, acme, "acme", false, nil)
	allow(k, acme, "globex", false, ErrForbidden)
	allow(k, acme, "", true, ErrForbidden)
	allow(k, admin, "globex", false, nil)
	allow(k, admin, "", true, nil)

	// The file holds hashes, never the keys, and a restart keeps them.
	b, _ := os.ReadFile(path)
	if strings.Contains(string(b), acme) || strings.Contains(string(b), admin) {
		t.Fatal("a key was saved in the clear")
	}
	k2, _ := New(nil)
	if err := k2.IssuedFrom(path); err != nil {
		t.Fatal(err)
	}
	if len(k2.List()) != 2 {
		t.Fatalf("reloaded %d keys, want 2", len(k2.List()))
	}
	allow(k2, acme, "acme", false, nil)

	if err := k2.Revoke(is.ID); err != nil {
		t.Fatal(err)
	}
	allow(k2, acme, "acme", false, ErrNoKey)
	if err := k2.Revoke(is.ID); err != ErrUnknownKeyID {
		t.Fatalf("second revoke: got %v", err)
	}
	k3, _ := New(nil)
	k3.IssuedFrom(path)
	allow(k3, acme, "acme", false, ErrNoKey)
	allow(k3, admin, "", true, nil)

	both, _, err := k3.Issue("both", []string{"acme", "globex"}, false)
	if err != nil {
		t.Fatal(err)
	}
	only, _, _ := k3.Issue("only", []string{"acme"}, false)
	if err := k3.DropTenant("acme"); err != nil {
		t.Fatal(err)
	}
	allow(k3, both, "acme", false, ErrForbidden)
	allow(k3, both, "globex", false, nil)
	allow(k3, only, "acme", false, ErrNoKey)
	allow(k3, admin, "acme", false, nil) // "*" keys are untouched
}
