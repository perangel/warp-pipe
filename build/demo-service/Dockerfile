FROM golang:1.17-alpine3.15 AS build

COPY . /go/src/github.com/perangel/warp-pipe
COPY build/demo-service/ /go/src/github.com/perangel/demo-service
WORKDIR /go/src/github.com/perangel/demo-service

RUN apk add --update git 
RUN go install .

CMD ["demo-service"]
