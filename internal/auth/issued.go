package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// ErrUnknownKeyID is returned by Revoke for an id that isn't issued.
var ErrUnknownKeyID = errors.New("auth: no issued key with that id")

// Issued describes a key handed out by Issue. Only a SHA-256 of the key is
// kept, so the key itself can't be read back from the server or its disk:
// whoever asked for it gets it once. The keys are 192 random bits, so a
// fast hash is enough; there's nothing to brute-force.
type Issued struct {
	ID      string    `json:"id"` // not secret; names the key for List and Revoke
	Name    string    `json:"name,omitempty"`
	Hash    string    `json:"hash"`
	Tenants []string  `json:"tenants"`
	Admin   bool      `json:"admin"`
	Created time.Time `json:"created"`
	Expires time.Time `json:"expires,omitzero"` // zero: never
}

func (is Issued) expired(now time.Time) bool {
	return !is.Expires.IsZero() && !now.Before(is.Expires)
}

// Limits on what Issue accepts, so the keys file stays small.
const (
	MaxKeyNameLen    = 128
	MaxKeyTenants    = 256
	MaxKeyTTL        = 10 * 365 * 24 * time.Hour
	maxTenantNameLen = 256
)

// Store holds the issued keys file. Load returns nil, nil if there isn't
// one yet.
type Store interface {
	Load() ([]byte, error)
	Save([]byte) error
}

// IssuedFrom is IssuedIn with the keys kept in a file at path.
func (k *Keys) IssuedFrom(path string) error {
	return k.IssuedIn(FileStore(path))
}

// IssuedIn loads keys issued earlier from store and makes Issue and Revoke
// save there. Call it once, before serving.
func (k *Keys) IssuedIn(store Store) error {
	b, err := store.Load()
	if err != nil {
		return err
	}
	issued := make(map[string]Issued)
	if b != nil {
		var list []Issued
		if err := json.Unmarshal(b, &list); err != nil {
			return fmt.Errorf("auth: issued keys: %w", err)
		}
		for _, is := range list {
			issued[is.Hash] = is
		}
	}
	k.wmu.Lock()
	defer k.wmu.Unlock()
	k.mu.Lock()
	defer k.mu.Unlock()
	k.store = store
	k.issued = issued
	return nil
}

// Issue creates a key for tenants (or for every tenant, as an admin key)
// that stops working after ttl (0: never), and returns it. It is the only
// time the key is available.
func (k *Keys) Issue(name string, tenants []string, admin bool, ttl time.Duration) (string, Issued, error) {
	switch {
	case !admin && len(tenants) == 0:
		return "", Issued{}, errors.New("auth: a non-admin key needs at least one tenant")
	case len(name) > MaxKeyNameLen:
		return "", Issued{}, fmt.Errorf("auth: key name is over %d bytes", MaxKeyNameLen)
	case len(tenants) > MaxKeyTenants:
		return "", Issued{}, fmt.Errorf("auth: a key can list at most %d tenants", MaxKeyTenants)
	case ttl < 0 || ttl > MaxKeyTTL:
		return "", Issued{}, errors.New("auth: ttl must be between 0 (never expires) and 10 years")
	}
	for _, t := range tenants {
		if t == "" || len(t) > maxTenantNameLen {
			return "", Issued{}, errors.New("auth: tenant names must be 1 to 256 bytes")
		}
	}
	if admin && len(tenants) == 0 {
		tenants = []string{"*"}
	}
	var secret [24]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "", Issued{}, err
	}
	key := "nk_" + base64.RawURLEncoding.EncodeToString(secret[:])
	hash := hashKey(key)
	is := Issued{
		ID:      "key_" + hash[:16],
		Name:    name,
		Hash:    hash,
		Tenants: slices.Clone(tenants),
		Admin:   admin,
		Created: time.Now().UTC().Truncate(time.Second),
	}
	if ttl > 0 {
		is.Expires = is.Created.Add(ttl)
	}

	k.wmu.Lock()
	defer k.wmu.Unlock()
	if k.issued == nil {
		return "", Issued{}, errors.New("auth: issued keys aren't enabled")
	}
	next := maps.Clone(k.issued)
	next[hash] = is
	if err := k.commit(next); err != nil {
		return "", Issued{}, err
	}
	return key, is, nil
}

// Revoke deletes the issued key named id. It stops working immediately.
func (k *Keys) Revoke(id string) error {
	k.wmu.Lock()
	defer k.wmu.Unlock()
	for hash, is := range k.issued {
		if is.ID == id {
			next := maps.Clone(k.issued)
			delete(next, hash)
			return k.commit(next)
		}
	}
	return ErrUnknownKeyID
}

// DropTenant removes tenant from every issued key, deleting keys left with
// none, so a tenant later re-created under the same name doesn't inherit
// keys meant for the old one.
func (k *Keys) DropTenant(tenant string) error {
	k.wmu.Lock()
	defer k.wmu.Unlock()
	next := maps.Clone(k.issued)
	changed := false
	for hash, is := range next {
		if !slices.Contains(is.Tenants, tenant) {
			continue
		}
		changed = true
		is.Tenants = slices.DeleteFunc(slices.Clone(is.Tenants), func(t string) bool { return t == tenant })
		if len(is.Tenants) == 0 {
			delete(next, hash)
		} else {
			next[hash] = is
		}
	}
	if !changed {
		return nil
	}
	return k.commit(next)
}

// List returns every issued key, oldest first.
func (k *Keys) List() []Issued {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return sorted(k.issued)
}

func sorted(issued map[string]Issued) []Issued {
	list := make([]Issued, 0, len(issued))
	for _, is := range issued {
		list = append(list, is)
	}
	slices.SortFunc(list, func(a, b Issued) int {
		if c := a.Created.Compare(b.Created); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	return list
}

// commit saves next and then makes it the live set, so a failed save
// changes nothing. Caller holds wmu.
func (k *Keys) commit(next map[string]Issued) error {
	b, err := json.MarshalIndent(sorted(next), "", "  ")
	if err != nil {
		return err
	}
	if err := k.store.Save(b); err != nil {
		return err
	}
	k.mu.Lock()
	k.issued = next
	k.mu.Unlock()
	return nil
}

// FileStore keeps issued keys in a file, readable only by its owner.
type FileStore string

func (path FileStore) Load() ([]byte, error) {
	b, err := os.ReadFile(string(path))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

// Save writes to a temp file and renames it over path, so a crash leaves
// either the old list or the new one.
func (path FileStore) Save(b []byte) error {
	tmp := string(path) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, string(path)); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(string(path)))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
