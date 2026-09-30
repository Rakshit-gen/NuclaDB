package replication

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestBlobRoundTrip(t *testing.T) {
	var wire bytes.Buffer
	for _, body := range []string{"snapshot bytes", "meta"} {
		path := filepath.Join(t.TempDir(), "blob")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := writeBlob(&wire, f); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}

	// Two blobs back to back, read in order through separate readers.
	for _, want := range []string{"snapshot bytes", "meta"} {
		got, err := io.ReadAll(&blobReader{r: &wire})
		if err != nil || string(got) != want {
			t.Fatalf("got %q, %v; want %q", got, err, want)
		}
	}

	// Declared 16 EiB, only 2 bytes follow: must fail, not read forever or
	// allocate the declared size.
	short := bytes.NewReader([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1, 2})
	if _, err := io.ReadAll(&blobReader{r: short}); err != io.ErrUnexpectedEOF {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
}
