package raft

import "testing"

func TestTCPTransportRefusesNonLoopback(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:0", ":0"} {
		if tr, err := NewTCPTransport(addr); err == nil {
			tr.Close()
			t.Fatalf("%s: transport created, want refusal", addr)
		}
	}
	tr, err := NewTCPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tr.Close()
}
