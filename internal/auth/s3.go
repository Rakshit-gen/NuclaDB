package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// S3Store keeps issued keys in one object in an S3-compatible bucket
// (Cloudflare R2, AWS S3, Backblaze B2 and so on), for hosts with no
// lasting disk. URL names the object, path-style, for example
// https://<account>.r2.cloudflarestorage.com/<bucket>/api_keys.json.
type S3Store struct {
	URL       string
	Region    string // "auto" for R2
	AccessKey string
	SecretKey string
	Client    *http.Client
}

func (s *S3Store) Load() ([]byte, error) {
	resp, err := s.do(http.MethodGet, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auth: loading issued keys: %s: %s", resp.Status, bytes.TrimSpace(b))
	}
	return b, nil
}

func (s *S3Store) Save(b []byte) error {
	resp, err := s.do(http.MethodPut, b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("auth: saving issued keys: %s: %s", resp.Status, bytes.TrimSpace(msg))
	}
	return nil
}

func (s *S3Store) do(method string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest(method, s.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	sign(req, body, s.Region, s.AccessKey, s.SecretKey, time.Now())
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return client.Do(req)
}

// sign adds AWS Signature Version 4 headers to req, signing host and every
// header already set on it.
func sign(req *http.Request, body []byte, region, accessKey, secretKey string, now time.Time) {
	sum := sha256.Sum256(body)
	payload := hex.EncodeToString(sum[:])
	stamp := now.UTC().Format("20060102T150405Z")
	day := stamp[:8]
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payload)

	headers := map[string]string{"host": req.URL.Host}
	for name, v := range req.Header {
		headers[strings.ToLower(name)] = strings.TrimSpace(strings.Join(v, ","))
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	slices.Sort(names)
	var canonHeaders strings.Builder
	for _, name := range names {
		canonHeaders.WriteString(name + ":" + headers[name] + "\n")
	}
	signed := strings.Join(names, ";")

	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canonical := strings.Join([]string{
		req.Method, path, canonicalQuery(req.URL.Query()), canonHeaders.String(), signed, payload,
	}, "\n")
	scope := day + "/" + region + "/s3/aws4_request"
	creq := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(creq[:])

	key := []byte("AWS4" + secretKey)
	for _, part := range []string{day, region, "s3", "aws4_request"} {
		key = hmacSHA256(key, part)
	}
	sig := hex.EncodeToString(hmacSHA256(key, toSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+accessKey+"/"+scope+
		", SignedHeaders="+signed+", Signature="+sig)
}

func canonicalQuery(q url.Values) string {
	// url.Values.Encode sorts by key but escapes spaces as "+"; SigV4 wants %20.
	return strings.ReplaceAll(q.Encode(), "+", "%20")
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}
