FROM golang:alpine

COPY . /go/src/github.com/perangel/warp-pipe
COPY build/demo-service/ /go/src/github.com/perangel/warp-pipe-demo
WORKDIR /go/src/github.com/perangel/warp-pipe-demo

RUN apk add --update git && \
    go get gopkg.in/olahol/melody.v1 && \
    go install .

CMD ["warp-pipe-demo"]
