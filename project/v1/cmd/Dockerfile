# Build
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY main.go .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o wikistream_project main.go

# Actual runtime
FROM gcr.io/distroless/base-debian12:nonroot

WORKDIR /app
COPY --from=builder /app/wikistream_project /app/wikistream_project

EXPOSE 7000
ENTRYPOINT ["/app/wikistream_project"]