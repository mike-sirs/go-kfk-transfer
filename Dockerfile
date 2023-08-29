FROM golang:1.21-alpine as base
WORKDIR /app
COPY go.* ./
RUN go mod download -x

FROM base AS build
ENV CGO_ENABLED=0
WORKDIR /app

COPY . /app
RUN go mod tidy
RUN go build -o dist/go-kfk-transfer_amd64_linux ./cmd

FROM google/cloud-sdk:alpine
COPY --from=build /app/dist/go-kfk-transfer_amd64_linux /usr/local/bin/go-kfk-transfer
RUN chmod +x /usr/local/bin/go-kfk-transfer

CMD ["go-kfk-transfer"]
