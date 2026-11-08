# syntax=docker/dockerfile:1
# One image, many roles: `boomerang <api|fanout|worker|retry|recorder|realtime|shopsim|receivers|init>`.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/boomerang ./cmd/boomerang

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget && adduser -D -u 10001 boomerang
COPY --from=build /out/boomerang /usr/local/bin/boomerang
USER boomerang
EXPOSE 8080
ENTRYPOINT ["boomerang"]
