package main

import (
	"testing"

	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

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

func TestParseWhere(t *testing.T) {
	f, err := parseWhere("team:in:search,ads")
	if err != nil || f.GetKey() != "team" || f.GetOp() != pb.FilterOp_FILTER_OP_IN || len(f.GetValues()) != 2 {
		t.Fatalf("in clause = %v, %v", f, err)
	}
	f, err = parseWhere("year:gte:2024")
	if err != nil || f.GetOp() != pb.FilterOp_FILTER_OP_GTE || f.GetValue() != "2024" {
		t.Fatalf("gte clause = %v, %v", f, err)
	}
	f, err = parseWhere("color:not_in:red")
	if err != nil || f.GetOp() != pb.FilterOp_FILTER_OP_NOT_IN || f.GetValues()[0] != "red" {
		t.Fatalf("not_in clause = %v, %v", f, err)
	}
	if f, err = parseWhere("color:exists"); err != nil || f.GetOp() != pb.FilterOp_FILTER_OP_EXISTS {
		t.Fatalf("exists clause = %v, %v", f, err)
	}
	for _, bad := range []string{"color", "color:like:x"} {
		if _, err := parseWhere(bad); err == nil {
			t.Errorf("parseWhere(%q) = nil error", bad)
		}
	}
	if _, err := parseMetric("manhattan"); err == nil {
		t.Error("parseMetric(manhattan) = nil error")
	}
	if m, err := parseMetric("L2"); err != nil || m != pb.DistanceMetric_DISTANCE_METRIC_L2 {
		t.Errorf("parseMetric(L2) = %v, %v", m, err)
	}
}
