FROM golang:1.14-alpine3.11 AS build

COPY . /go/src/github.com/perangel/warp-pipe
COPY build/demo-service/ /go/src/github.com/perangel/demo-service
WORKDIR /go/src/github.com/perangel/demo-service

RUN apk add --update git 
RUN go install .

CMD ["demo-service"]
