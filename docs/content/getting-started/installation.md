---
title: "Installation"
description: "Install the tatami CLI from a package manager, a prebuilt binary, Go, or the container image, and add the library to a Go module."
weight: 20
---

tatami ships as a single static binary with no runtime dependencies, and as a Go library. Pick whichever fits.

## Homebrew (macOS and Linux)

```bash
brew install tamnd/tap/tatami
```

## Scoop (Windows)

```bash
scoop bucket add tamnd https://github.com/tamnd/scoop-bucket
scoop install tatami
```

## Prebuilt binary

Every release publishes archives for Linux, macOS, Windows, and FreeBSD on both amd64 and arm64, along with `.deb`, `.rpm`, and `.apk` packages, checksums, SBOMs, and a cosign signature. Download the one for your platform from the [releases page](https://github.com/tamnd/tatami/releases/latest), unpack it, and put the `tatami` binary on your `PATH`.

On a Debian or Ubuntu machine:

```bash
curl -LO https://github.com/tamnd/tatami/releases/latest/download/tatami_<version>_linux_amd64.deb
sudo dpkg -i tatami_<version>_linux_amd64.deb
```

## Go install

With a Go toolchain (1.24 or newer):

```bash
go install github.com/tamnd/tatami/cmd/tatami@latest
```

This puts `tatami` in `$(go env GOPATH)/bin`.

## Container image

A multi-arch image is published to the GitHub Container Registry:

```bash
docker run --rm -v "$PWD:/data" ghcr.io/tamnd/tatami:latest inspect /data/shard.tatami
```

## The Go library

tatami is a library first; the CLI is a thin wrapper over it. Add it to a module:

```bash
go get github.com/tamnd/tatami
```

The format library itself is dependency-free apart from the zstd codec. The `convert` package, which reads Parquet, is the only part that pulls in [parquet-go](https://github.com/parquet-go/parquet-go), so a program that only writes and reads `.tatami` files stays lean.

## Verify

```bash
tatami --version
```

Next: [the quick start](/getting-started/quick-start/).
