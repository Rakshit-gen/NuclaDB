package auth

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// The GET Object example from the AWS SigV4 docs ("Examples: Signature
// Calculations in AWS Signature Version 4").
func TestSignMatchesAWSExample(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	req.Header.Set("Range", "bytes=0-9")
	sign(req, nil, "us-east-1", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC))
	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, " +
		"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestS3StoreKeepsIssuedKeys(t *testing.T) {
	var mu sync.Mutex
	var object []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=ak/") {
			http.Error(w, "unsigned", http.StatusForbidden)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			if object == nil {
				http.NotFound(w, r)
				return
			}
			w.Write(object)
		case http.MethodPut:
			object, _ = io.ReadAll(r.Body)
		}
	}))
	defer srv.Close()
	store := &S3Store{URL: srv.URL + "/bucket/api_keys.json", Region: "auto", AccessKey: "ak", SecretKey: "sk"}

	k, _ := New(nil)
	if err := k.IssuedIn(store); err != nil {
		t.Fatal(err)
	}
	key, _, err := k.Issue("ci", []string{"acme"}, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(object), key) {
		t.Fatal("a key was saved in the clear")
	}

	// A fresh server, as after a redeploy, gets the key back from the bucket.
	k2, _ := New(nil)
	if err := k2.IssuedIn(store); err != nil {
		t.Fatal(err)
	}
	if err := k2.Allow(WithKey(t.Context(), key), "acme", false); err != nil {
		t.Fatalf("reloaded key: %v", err)
	}

	bad := &S3Store{URL: store.URL, Region: "auto", AccessKey: "wrong", SecretKey: "sk"}
	if err := bad.Save([]byte("[]")); err == nil {
		t.Fatal("a refused save reported success")
	}
	// A refused load must fail, not start empty and overwrite the bucket on
	// the next Issue.
	k3, _ := New(nil)
	if err := k3.IssuedIn(bad); err == nil {
		t.Fatal("a refused load reported success")
	}
}
