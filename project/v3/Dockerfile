# Build
ARG SERVICE=consumer
FROM golang:1.27.2-alpine3.24 AS builder
ARG SERVICE

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/wikistream ./cmd/${SERVICE}

# Actual runtime
FROM gcr.io/distroless/base-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/wikistream /app/wikistream

EXPOSE 7000
ENTRYPOINT ["/app/wikistream"]