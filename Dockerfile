FROM golang:1.21-alpine AS builder
RUN apk add --no-cache git
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o anomaly-detector .

FROM alpine:3.19
# tzdata required for time.LoadLocation() to work with non-UTC timezones
RUN apk add --no-cache tzdata ca-certificates
WORKDIR /app
COPY --from=builder /build/anomaly-detector .
EXPOSE 9091
ENTRYPOINT ["./anomaly-detector", "config.ini"]
