FROM golang:1.22-alpine AS builder
WORKDIR /src

# Copy dependency manifests first so Docker can cache the module download
# layer independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically-linked binary.
# -trimpath    removes local filesystem paths from the binary (cleaner stack traces)
# -ldflags     strips debug info (-s) and DWARF (-w) to reduce image size
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /mca \
      .

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — runtime image
# scratch = zero OS packages, zero attack surface.
# ca-certificates is copied from the builder so the Go TLS stack can verify
# MongoDB Atlas / TLS-enabled cluster certificates.
# ─────────────────────────────────────────────────────────────────────────────
FROM scratch

# TLS root certificates (needed for Atlas / TLS clusters)
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# The binary
COPY --from=builder /mca /mca

# The HTML template (read at startup, relative to the working directory)
COPY templates/index.html /data/templates/index.html

# Working directory: the template is resolved relative to it, and with --tls
# the auto-generated self-signed cert (mca.crt / mca.key) is written here.
# Mount a volume at /data/certs (or /data) if you want certs to persist.
WORKDIR /data

# ── Runtime configuration ────────────────────────────────────────────────────
# MClusterAdmin is configured purely via CLI flags (no env vars). Pass them
# as container args (Docker: after the image name; K8s: via `args:`):
#
#   --tcp_port <port>   TCP port to listen on              (default: 8787)
#   --tls               Enable HTTPS (self-signed if no cert/key provided)
#   --cert <path>       Path to TLS certificate PEM        (default: mca.crt)
#   --key <path>        Path to TLS private key PEM        (default: mca.key)
#   --debug             Enable server-side debug logging
#   --view-only         Read-only mode — all write operations disabled
#
# Example:
#   docker run -p 8787:8787 mclusteradmin --tls --view-only
EXPOSE 8787

ENTRYPOINT ["/mca"]
CMD []
