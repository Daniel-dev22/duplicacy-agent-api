FROM golang:1.25-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git
COPY go.mod go.sum* ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o duplicacy-agent-api .

FROM alpine:3.21
ARG DUPLICACY_VERSION=3.2.5
ARG TARGETARCH=amd64
RUN apk add --no-cache ca-certificates curl tzdata
RUN ARCH=$(case "$TARGETARCH" in amd64) echo "x64";; arm64) echo "arm64";; *) echo "x64";; esac) && \
    curl -fsSL -o /usr/local/bin/duplicacy \
      "https://github.com/gilbertchen/duplicacy/releases/download/v${DUPLICACY_VERSION}/duplicacy_linux_${ARCH}_${DUPLICACY_VERSION}" && \
    chmod +x /usr/local/bin/duplicacy && \
    /usr/local/bin/duplicacy | head -1
COPY --from=builder /app/duplicacy-agent-api /usr/local/bin/duplicacy-agent-api
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -sf http://localhost:8080/health/ready || exit 1
ENTRYPOINT ["duplicacy-agent-api"]
