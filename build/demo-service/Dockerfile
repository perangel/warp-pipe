FROM golang:1.14-alpine3.11 AS build

COPY . /go/src/github.com/perangel/warp-pipe
COPY build/demo-service/ /go/src/github.com/perangel/warp-pipe-demo
WORKDIR /go/src/github.com/perangel/warp-pipe-demo

RUN apk add --update git && \
    go install .

CMD ["/go/bin/warp-pipe-demo"]
