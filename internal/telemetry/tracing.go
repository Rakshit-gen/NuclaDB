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
// If OTEL_EXPORTER_OTLP_ENDPOINT is set, spans are exported via OTLP/gRPC
// to that collector (the standard OTel SDK env var, honored automatically
// by otlptracegrpc.New with no explicit options). Otherwise spans are
// written to stdout — so tracing is inspectable with zero external
// dependencies out of the box, and upgrades to a real collector by
// setting one environment variable, not a code change.
func SetupTracing(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error) {
	exporter, err := newExporter(ctx)
	if err != nil {
		return nil, err
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

func newExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	if endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); endpoint != "" {
		return otlptracegrpc.New(ctx)
	}
	return stdouttrace.New(stdouttrace.WithPrettyPrint())
}
