# ---------------------------------------------------------------------------
# Hotelier — Minimal Containerfile
# ---------------------------------------------------------------------------
#
# Build:  podman build -t hotelier .
# Run:    podman run -d --name hotelier \
#           -p 8080:8080 \
#           -v /path/to/config:/etc/hotelier:Z,ro \
#           -v /path/to/logs:/var/log/hotelier:Z \
#           hotelier:latest
# ---------------------------------------------------------------------------
FROM golang:1.26-trixie AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
ARG LDFLAGS="-s -w"
RUN CGO_ENABLED=0 go build -trimpath -ldflags "${LDFLAGS}" -o /usr/local/bin/hotelier ./cmd/hotelier
RUN mkdir -p /var/log/hotelier

# ---------------------------------------------------------------------------
# runtime — scratch image with the binary
# ---------------------------------------------------------------------------
FROM scratch

COPY --from=builder /usr/local/bin/hotelier /hotelier
COPY --from=builder /var/log/hotelier /var/log/hotelier

WORKDIR /etc/hotelier

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD /hotelier --config /etc/hotelier/server.yaml || exit 1

ENTRYPOINT ["/hotelier"]
CMD ["--config", "/etc/hotelier/server.yaml"]
