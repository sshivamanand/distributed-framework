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
ENTRYPOINT ["leader"]

FROM alpine:3.20 AS worker
COPY --from=builder /out/worker /usr/local/bin/worker
ENTRYPOINT ["worker"]

FROM alpine:3.20 AS client
COPY --from=builder /out/client /usr/local/bin/client
ENTRYPOINT ["client"]
