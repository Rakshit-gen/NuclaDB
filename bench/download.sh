#!/usr/bin/env bash
# Downloads everything the benchmark needs that isn't checked into the
# repo: the standard SIFT-small ANN dataset and a native Qdrant binary.
# Neither is committed — the dataset is a third-party corpus, and the
# Qdrant binary is a 70MB+ platform-specific executable.
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Downloading siftsmall dataset (texmex corpus)..."
mkdir -p data
if [ ! -f data/siftsmall/siftsmall_base.fvecs ]; then
  curl -sL -o data/siftsmall.tar.gz "ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz"
  tar xzf data/siftsmall.tar.gz -C data
  rm data/siftsmall.tar.gz
else
  echo "    already present, skipping"
fi

echo "==> Downloading Qdrant binary..."
mkdir -p .qdrant-bin
if [ ! -x .qdrant-bin/qdrant ]; then
  OS="$(uname -s)"
  ARCH="$(uname -m)"
  case "$OS-$ARCH" in
    Darwin-arm64) TARGET="aarch64-apple-darwin" ;;
    Darwin-x86_64) TARGET="x86_64-apple-darwin" ;;
    Linux-x86_64) TARGET="x86_64-unknown-linux-gnu" ;;
    Linux-aarch64) TARGET="aarch64-unknown-linux-gnu" ;;
    *) echo "unsupported platform $OS-$ARCH; download a Qdrant release manually from https://github.com/qdrant/qdrant/releases" >&2; exit 1 ;;
  esac
  VERSION="$(curl -s https://api.github.com/repos/qdrant/qdrant/releases/latest | grep '"tag_name"' | cut -d'"' -f4)"
  curl -sL -o .qdrant-bin/qdrant.tar.gz "https://github.com/qdrant/qdrant/releases/download/${VERSION}/qdrant-${TARGET}.tar.gz"
  tar xzf .qdrant-bin/qdrant.tar.gz -C .qdrant-bin
  rm .qdrant-bin/qdrant.tar.gz
  chmod +x .qdrant-bin/qdrant
else
  echo "    already present, skipping"
fi

echo "==> Done. Run: go build -o ../bin/nucladbd ../cmd/nucladbd && go run ./cmd/compare"
