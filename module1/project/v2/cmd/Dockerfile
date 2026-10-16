# Build
FROM golang:1.27.2-alpine3.24 AS builder

WORKDIR /app
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o wikistream_project .

# Actual runtime
FROM gcr.io/distroless/base-debian12:nonroot

WORKDIR /app
COPY --from=builder /app/wikistream_project /app/wikistream_project

EXPOSE 7000
ENTRYPOINT ["/app/wikistream_project"]