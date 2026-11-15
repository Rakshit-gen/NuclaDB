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
}

// IssuedFrom loads keys issued earlier from path, if it exists, and makes
// Issue and Revoke save there. Call it once, before serving.
func (k *Keys) IssuedFrom(path string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.path = path
	k.issued = make(map[string]Issued)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var list []Issued
	if err := json.Unmarshal(b, &list); err != nil {
		return fmt.Errorf("auth: %s: %w", path, err)
	}
	for _, is := range list {
		k.issued[is.Hash] = is
	}
	return nil
}

// Issue creates a key for tenants (or for every tenant, as an admin key)
// and returns it. It is the only time the key is available.
func (k *Keys) Issue(name string, tenants []string, admin bool) (string, Issued, error) {
	if !admin && len(tenants) == 0 {
		return "", Issued{}, errors.New("auth: a non-admin key needs at least one tenant")
	}
	if slices.Contains(tenants, "") {
		return "", Issued{}, errors.New("auth: empty tenant name")
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

	k.mu.Lock()
	defer k.mu.Unlock()
	if k.issued == nil {
		return "", Issued{}, errors.New("auth: issued keys aren't enabled")
	}
	k.issued[hash] = is
	if err := k.saveLocked(); err != nil {
		delete(k.issued, hash)
		return "", Issued{}, err
	}
	return key, is, nil
}

// Revoke deletes the issued key named id. It stops working immediately.
func (k *Keys) Revoke(id string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for hash, is := range k.issued {
		if is.ID == id {
			delete(k.issued, hash)
			if err := k.saveLocked(); err != nil {
				k.issued[hash] = is
				return err
			}
			return nil
		}
	}
	return ErrUnknownKeyID
}

// DropTenant removes tenant from every issued key, deleting keys left with
// none, so a tenant later re-created under the same name doesn't inherit
// keys meant for the old one.
func (k *Keys) DropTenant(tenant string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	before := maps.Clone(k.issued)
	changed := false
	for hash, is := range k.issued {
		if !slices.Contains(is.Tenants, tenant) {
			continue
		}
		changed = true
		is.Tenants = slices.DeleteFunc(slices.Clone(is.Tenants), func(t string) bool { return t == tenant })
		if len(is.Tenants) == 0 {
			delete(k.issued, hash)
		} else {
			k.issued[hash] = is
		}
	}
	if !changed {
		return nil
	}
	if err := k.saveLocked(); err != nil {
		k.issued = before
		return err
	}
	return nil
}

// List returns every issued key, oldest first.
func (k *Keys) List() []Issued {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.sortedLocked()
}

func (k *Keys) sortedLocked() []Issued {
	list := make([]Issued, 0, len(k.issued))
	for _, is := range k.issued {
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

// saveLocked writes every issued key to a temp file and renames it over
// k.path, so a crash leaves either the old list or the new one.
func (k *Keys) saveLocked() error {
	b, err := json.MarshalIndent(k.sortedLocked(), "", "  ")
	if err != nil {
		return err
	}
	tmp := k.path + ".tmp"
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
	if err := os.Rename(tmp, k.path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(k.path))
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
