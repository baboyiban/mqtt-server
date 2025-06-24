FROM golang:1.24.4-alpine AS builder

WORKDIR /app
COPY . .

RUN go mod tidy
RUN go build -o mqtt-server ./cmd

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/mqtt-server .
EXPOSE 1883

CMD ["./mqtt-server"]
