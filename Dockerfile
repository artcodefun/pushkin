FROM golang:1.27.1-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/pushkin ./cmd/server
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/fake-fcm ./cmd/fake-fcm

FROM alpine:3.21 AS migrate-downloader

ARG TARGETARCH
ARG MIGRATE_VERSION=v4.19.1
RUN case "${TARGETARCH:-}" in \
      "" | amd64 | x86_64) migrate_arch=amd64 ;; \
      arm64 | aarch64) migrate_arch=arm64 ;; \
      *) echo "unsupported migration architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    apk add --no-cache curl tar && \
    curl --fail --location --silent --show-error \
      "https://github.com/golang-migrate/migrate/releases/download/${MIGRATE_VERSION}/migrate.linux-${migrate_arch}.tar.gz" | \
      tar --extract --gzip --file - --directory /usr/local/bin migrate

FROM alpine:3.21

WORKDIR /app
RUN apk add --no-cache ca-certificates && addgroup --system pushkin && adduser --system --ingroup pushkin pushkin

COPY --from=builder /out/pushkin /app/pushkin
COPY --from=builder /out/fake-fcm /app/fake-fcm
COPY --from=migrate-downloader /usr/local/bin/migrate /usr/local/bin/migrate
COPY internal/infrastructure/postgres/migrations /app/migrations
COPY scripts/migrate.sh /app/migrate.sh
RUN chmod 0755 /app/pushkin /app/fake-fcm /app/migrate.sh /usr/local/bin/migrate

USER pushkin
EXPOSE 8080
CMD ["/app/pushkin"]
