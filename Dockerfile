# syntax=docker/dockerfile:1.7
#
# BuildKit syntax directive required for the `--mount=type=secret` `go mod
# download` below. The private agent-kit-go module is fetched using the GitHub
# App token the build-agent provides as the `ghtoken` secret — a credential
# helper reads it from the tmpfs mount at fetch time, so the token value never
# lands in .gitconfig or any layer (the buildcache is pushed to the registry).
FROM golang:1.26-alpine AS builder
# Target Intel Haswell+ / AMD Excavator+ for FMA/AVX2 wins. Override
# with --build-arg GOAMD64=v1 for older CPUs. No-op for arm64 builds.
ARG GOAMD64=v3
ENV GOAMD64=${GOAMD64}
ARG DUPLICACY_VERSION=3.2.5
WORKDIR /app
RUN apk add --no-cache git curl
# Bypass the public proxy for our private GitHub org so go mod download hits git
# directly (the credential helper supplies the token over HTTPS).
ENV GOPRIVATE=github.com/Daniel-dev22/*
COPY go.mod go.sum* ./
# required=true so a build without the token fails loudly rather than silently
# attempting an anonymous (404) fetch of the private module.
RUN --mount=type=secret,id=ghtoken,required=true \
    git config --global credential.helper '!f() { echo username=x-access-token; echo "password=$(cat /run/secrets/ghtoken)"; }; f' && \
    go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o duplicacy-agent-api .

# Fetch the duplicacy CLI HERE in the builder, where `go env GOARCH`
# authoritatively reports the TARGET arch — the builder runs under the target's
# qemu, which is exactly how the Go binary above already gets the right arch.
# The runtime stage's BuildKit TARGETARCH arg resolved to "amd64" even for
# arm64 builds in our buildx setup, shipping an amd64 duplicacy into the arm64
# image (exec-format-error on real Pis). Sourcing arch from `go env` + asserting
# the ELF e_machine byte (no exec, so qemu can't mask a mismatch) makes that
# impossible. e_machine: x86-64=0x3e, aarch64=0xb7.
RUN set -eux; \
    GOARCH=$(go env GOARCH); \
    case "$GOARCH" in \
      amd64) DARCH=x64;   EXPECT=3e ;; \
      arm64) DARCH=arm64; EXPECT=b7 ;; \
      *) echo "unsupported GOARCH=$GOARCH"; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/duplicacy \
      "https://github.com/gilbertchen/duplicacy/releases/download/v${DUPLICACY_VERSION}/duplicacy_linux_${DARCH}_${DUPLICACY_VERSION}"; \
    chmod +x /tmp/duplicacy; \
    GOT=$(dd if=/tmp/duplicacy bs=1 skip=18 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n'); \
    echo "duplicacy ELF e_machine=0x${GOT} expect=0x${EXPECT} (GOARCH=${GOARCH}, DARCH=${DARCH})"; \
    [ "$GOT" = "$EXPECT" ] || { echo "FATAL: duplicacy CLI arch mismatch (GOARCH=${GOARCH})"; exit 1; }

FROM alpine:3.21
RUN apk add --no-cache ca-certificates curl tzdata
COPY --from=builder /tmp/duplicacy /usr/local/bin/duplicacy
COPY --from=builder /app/duplicacy-agent-api /usr/local/bin/duplicacy-agent-api
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -sf http://localhost:8080/health/ready || exit 1
ENTRYPOINT ["duplicacy-agent-api"]
