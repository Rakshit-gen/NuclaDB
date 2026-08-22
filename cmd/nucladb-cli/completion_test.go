package main

import "testing"

func TestRunCompletion(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish"} {
		if err := runCompletion([]string{shell}); err != nil {
			t.Errorf("runCompletion(%q) = %v, want nil", shell, err)
		}
	}
	if err := runCompletion([]string{"powershell"}); err == nil {
		t.Error("runCompletion(\"powershell\") = nil, want an error")
	}
	if err := runCompletion(nil); err == nil {
		t.Error("runCompletion(nil) = nil, want an error")
	}
}

func TestFreeAddr(t *testing.T) {
	a, err := freeAddr()
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	b, err := freeAddr()
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	if a == b {
		t.Errorf("freeAddr returned the same address twice: %s", a)
	}
}
