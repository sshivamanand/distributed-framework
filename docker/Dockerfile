# One builder stage compiles all three binaries; the leader/worker/client
# stages below each copy out just their own, so docker-compose.yml can
# build each service with `target: leader` / `worker` / `client` without
# repeating the build step three times across three separate files.

FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /out/leader ./cmd/leader
RUN CGO_ENABLED=0 go build -o /out/worker ./cmd/worker
RUN CGO_ENABLED=0 go build -o /out/client ./cmd/client

FROM alpine:3.20 AS leader
COPY --from=builder /out/leader /usr/local/bin/leader
COPY docker/leader-entrypoint.sh /usr/local/bin/leader-entrypoint.sh
RUN chmod +x /usr/local/bin/leader-entrypoint.sh
ENTRYPOINT ["leader-entrypoint.sh"]

FROM alpine:3.20 AS worker
COPY --from=builder /out/worker /usr/local/bin/worker
COPY docker/worker-entrypoint.sh /usr/local/bin/worker-entrypoint.sh
RUN chmod +x /usr/local/bin/worker-entrypoint.sh
ENTRYPOINT ["worker-entrypoint.sh"]

FROM alpine:3.20 AS client
COPY --from=builder /out/client /usr/local/bin/client
ENTRYPOINT ["client"]
