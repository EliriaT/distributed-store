# first stage - builds the binary from sources
FROM golang:alpine as build

WORKDIR /app

COPY . .

RUN go mod download && go mod verify

RUN go build -o main .

# second stage - using minimal image to run the server
FROM alpine:latest

WORKDIR /node

COPY --from=build /app/main .
COPY --from=build /app/sharding.toml .
COPY --from=build /app/config/env/* ./config/env/

CMD ["./main", "-db-location=database/chisinau", "-http-addr=127.0.0.0:8080", "-config-file=sharding.toml", "-shard=Chisinau", "-env=config/env/.env0" ]

EXPOSE 8080