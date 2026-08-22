#!/bin/sh
# Installs nucladb-cli with one command:
#
#   curl -fsSL https://raw.githubusercontent.com/Rakshit-gen/NuclaDB/main/install.sh | sh
#
# This just wraps `go install` rather than shipping prebuilt binaries —
# NuclaDB doesn't cut tagged releases yet, and `go install ...@latest`
# already resolves to the latest commit on the default branch without one,
# so a cross-compiled binary pipeline would be duplicating what the Go
# toolchain does for free.
set -eu

if ! command -v go >/dev/null 2>&1; then
  echo "nucladb-cli needs the Go toolchain to install (go install under the hood)." >&2
  echo "Install Go from https://go.dev/dl/, then re-run this script." >&2
  exit 1
fi

echo "Installing nucladb-cli (go install github.com/Rakshit-gen/nucladb/cmd/nucladb-cli@latest)..."
go install github.com/Rakshit-gen/nucladb/cmd/nucladb-cli@latest

bin_dir=$(go env GOBIN)
if [ -z "$bin_dir" ]; then
  bin_dir="$(go env GOPATH)/bin"
fi

echo "Installed to $bin_dir/nucladb-cli"
case ":$PATH:" in
  *":$bin_dir:"*) ;;
  *) echo "Add it to your PATH: export PATH=\"$bin_dir:\$PATH\"" ;;
esac
echo "Then: nucladb-cli ping   (with NUCLADB_ADDR pointed at a running nucladbd)"
