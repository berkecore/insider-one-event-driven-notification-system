# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mockprovider ./cmd/mockprovider

FROM alpine:3.21 AS server

RUN apk add --no-cache ca-certificates tzdata wget && \
    adduser -D -H -u 10001 appuser

COPY --from=builder /out/server /usr/local/bin/server

USER appuser
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://localhost:8080/health || exit 1

ENTRYPOINT ["/usr/local/bin/server"]

FROM alpine:3.21 AS mockprovider

RUN apk add --no-cache ca-certificates tzdata wget && \
    adduser -D -H -u 10001 appuser

COPY --from=builder /out/mockprovider /usr/local/bin/mockprovider

USER appuser
EXPOSE 8081

HEALTHCHECK --interval=10s --timeout=3s --start-period=3s --retries=3 \
  CMD wget -qO- http://localhost:8081/health || exit 1

ENTRYPOINT ["/usr/local/bin/mockprovider"]
