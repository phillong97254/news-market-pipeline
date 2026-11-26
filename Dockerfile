# Build trên chính Oracle (arm64): không đặt GOARCH, Go tự lấy theo máy build.
# Dùng chung cho mọi app trong cmd/: docker build --build-arg APP=price-producer .
# Go 1.27.4 (bản vá mới nhất; 1.27.1 còn 10 lỗ hổng thư viện chuẩn). Trước đó ở 1.26.1 govulncheck báo 25 lỗ hổng thư viện chuẩn (net/http, crypto/tls, x509...).
FROM golang:1.27.4-alpine AS build
ARG APP
WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/app ./cmd/${APP}

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata && adduser -D -H app
USER app
COPY --from=build /out/app /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
