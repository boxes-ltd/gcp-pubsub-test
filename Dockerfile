FROM golang:1.22.8 AS builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . .

RUN go build -o main .

FROM gcr.io/distroless/base

COPY --from=builder /app/main .

CMD ["/main"]