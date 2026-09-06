// Package telemetry wires OpenTelemetry tracing and Prometheus metrics
// into the server: every gRPC call gets a trace span (via otelgrpc's
// stats handler) and is counted/timed into Prometheus histograms, plus a
// periodic sweep publishes per-tenant storage-quota usage as gauges.
package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// SetupTracing installs a global TracerProvider for serviceName and
// returns a shutdown func to flush and release it on server exit.
//
// Span export is off unless asked for, because a server that serializes
// and writes a span for every RPC pays that cost on the hot path whether
// or not anyone is looking at the traces:
//
//   - OTEL_EXPORTER_OTLP_ENDPOINT set -> export via OTLP/gRPC to that
//     collector (the standard OTel SDK env var).
//   - NUCLADB_TRACE_STDOUT set -> pretty-print spans to stdout, for
//     inspecting traces with zero external dependencies during development.
//   - neither -> no exporter installed; the otelgrpc handler falls back to
//     the no-op global tracer, so instrumentation stays in the code but
//     costs effectively nothing.
func SetupTracing(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error) {
	exporter, err := newExporter(ctx)
	if err != nil {
		return nil, err
	}
	if exporter == nil {
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceNameKey.String(serviceName),
	))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// newExporter returns nil, nil when no exporter is configured — see
// SetupTracing's doc comment for the three cases.
func newExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	switch {
	case os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "":
		return otlptracegrpc.New(ctx)
	case os.Getenv("NUCLADB_TRACE_STDOUT") != "":
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	default:
		return nil, nil
	}
}
