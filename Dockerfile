# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/nucladbd ./cmd/nucladbd
RUN CGO_ENABLED=0 go build -o /out/nucladb-cli ./cmd/nucladb-cli

FROM alpine:3.20
RUN adduser -D -H nucladb && mkdir -p /data && chown nucladb:nucladb /data
COPY --from=build /out/nucladbd /usr/local/bin/nucladbd
COPY --from=build /out/nucladb-cli /usr/local/bin/nucladb-cli
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
USER nucladb
VOLUME ["/data"]
EXPOSE 9090 8080
ENTRYPOINT ["docker-entrypoint.sh"]
