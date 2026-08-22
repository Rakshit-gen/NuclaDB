#!/bin/sh
# Render (and most PaaS platforms) assign the public HTTP port at runtime
# via $PORT rather than a fixed one — Docker's exec-form ENTRYPOINT can't
# expand that itself, so this script does it via a shell before exec'ing
# the real binary. gRPC stays on a fixed internal port since it isn't the
# platform's public-facing one; only the REST/JSON gateway needs to bind
# wherever $PORT points.
set -eu
exec nucladbd -data-dir=/data -grpc-addr=:9090 -http-addr=":${PORT:-8080}" "$@"
