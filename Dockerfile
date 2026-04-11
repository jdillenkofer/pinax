FROM golang:1.26.1-alpine3.22 AS app-builder

ARG SKIP_TESTS=false

RUN apk add --no-cache build-base

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN if [ "$SKIP_TESTS" = "false" ]; then go test ./... -v; fi

RUN adduser -D -u 10001 appuser
RUN mkdir -m 1777 /tmp-dir

RUN go install -ldflags='-linkmode external -s -w -extldflags "-static-pie"' -buildmode=pie ./cmd/pinax

RUN chown 10001:10001 /go/bin/pinax

FROM scratch

COPY --from=app-builder /go/bin/pinax /usr/local/bin/pinax
COPY --from=app-builder /etc/passwd /etc/passwd
COPY --from=app-builder /etc/ssl/certs /etc/ssl/certs
COPY --from=app-builder --chown=10001:10001 /tmp-dir /tmp

WORKDIR /app

EXPOSE 8000

USER 10001

ENTRYPOINT ["/usr/local/bin/pinax"]
