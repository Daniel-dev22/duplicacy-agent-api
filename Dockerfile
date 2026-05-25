# syntax=docker/dockerfile:1.7
#
# BuildKit syntax directive required for `RUN --mount=type=ssh` below, which
# we use to fetch the private agent-kit-go module via the host's SSH agent
# without baking the key into the image. Build invocation must pass
# `--ssh default` (ansible playbook handles this).
FROM golang:1.26-alpine AS builder
# Target Intel Haswell+ / AMD Excavator+ for FMA/AVX2 wins. Override
# with --build-arg GOAMD64=v1 for older CPUs. No-op for arm64 builds.
ARG GOAMD64=v3
ENV GOAMD64=${GOAMD64}
WORKDIR /app
RUN apk add --no-cache git openssh-client
# Bypass the public proxy for our private GitHub org so go mod download
# hits git directly. Rewrite https:// → git@ so it uses the SSH agent.
ENV GOPRIVATE=github.com/Daniel-dev22/*
RUN git config --global url."git@github.com:".insteadOf "https://github.com/" && \
    mkdir -p /root/.ssh && \
    ssh-keyscan -t rsa,ecdsa,ed25519 github.com >> /root/.ssh/known_hosts
COPY go.mod go.sum* ./
# Mount the SSH agent only for this RUN — key never lands in the image.
RUN --mount=type=ssh go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o duplicacy-agent-api .

FROM alpine:3.21
ARG DUPLICACY_VERSION=3.2.5
ARG TARGETARCH=amd64
RUN apk add --no-cache ca-certificates curl tzdata
# Fetch the duplicacy CLI for the TARGET arch and HARD-ASSERT the ELF arch
# matches. buildx's registry buildcache previously served a stale amd64
# duplicacy layer into the arm64 image — the old `duplicacy | head -1` check
# ran fine under qemu at build time but the binary exec-format-errored on real
# arm64 Pis. We now read the ELF e_machine byte (no exec, so qemu can't mask a
# mismatch) and fail the build on any mismatch; the new RUN text also
# invalidates the poisoned cache layer. e_machine: x86-64=0x3e, aarch64=0xb7.
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) DARCH=x64;   EXPECT=3e ;; \
      arm64) DARCH=arm64; EXPECT=b7 ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH"; exit 1 ;; \
    esac; \
    curl -fsSL -o /usr/local/bin/duplicacy \
      "https://github.com/gilbertchen/duplicacy/releases/download/v${DUPLICACY_VERSION}/duplicacy_linux_${DARCH}_${DUPLICACY_VERSION}"; \
    chmod +x /usr/local/bin/duplicacy; \
    GOT=$(dd if=/usr/local/bin/duplicacy bs=1 skip=18 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n'); \
    echo "duplicacy ELF e_machine=0x${GOT} expect=0x${EXPECT} (TARGETARCH=${TARGETARCH}, DARCH=${DARCH})"; \
    [ "$GOT" = "$EXPECT" ] || { echo "FATAL: duplicacy CLI arch mismatch for ${TARGETARCH}"; exit 1; }
COPY --from=builder /app/duplicacy-agent-api /usr/local/bin/duplicacy-agent-api
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -sf http://localhost:8080/health/ready || exit 1
ENTRYPOINT ["duplicacy-agent-api"]
