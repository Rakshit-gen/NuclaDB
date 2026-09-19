package replication

import (
	"bytes"
	"io"
	"testing"
)

func TestReadBlob(t *testing.T) {
	var b bytes.Buffer
	if err := writeBlob(&b, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := readBlob(&b)
	if err != nil || string(got) != "hello" {
		t.Fatalf("got %q, %v", got, err)
	}
	// Declared 4 GiB, only 2 bytes follow: must fail without allocating 4 GiB.
	if _, err := readBlob(bytes.NewReader([]byte{0xff, 0xff, 0xff, 0xff, 1, 2})); err != io.ErrUnexpectedEOF {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
}
