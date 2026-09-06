package telemetry

import (
	"context"
	"testing"
)

func TestNewExporterOffByDefault(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("NUCLADB_TRACE_STDOUT", "")

	exp, err := newExporter(context.Background())
	if err != nil {
		t.Fatalf("newExporter: %v", err)
	}
	if exp != nil {
		t.Fatalf("expected no exporter when nothing is configured, got %T", exp)
	}
}

func TestNewExporterStdoutOptIn(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("NUCLADB_TRACE_STDOUT", "1")

	exp, err := newExporter(context.Background())
	if err != nil {
		t.Fatalf("newExporter: %v", err)
	}
	if exp == nil {
		t.Fatal("expected a stdout exporter when NUCLADB_TRACE_STDOUT is set")
	}
}

func TestSetupTracingNoExporterReturnsUsableShutdown(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("NUCLADB_TRACE_STDOUT", "")

	shutdown, err := SetupTracing(context.Background(), "test")
	if err != nil {
		t.Fatalf("SetupTracing: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
