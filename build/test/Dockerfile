FROM golang:1.14-alpine3.11 AS build
COPY . /go/src/github.com/perangel/warp-pipe
WORKDIR /go/src/github.com/perangel/warp-pipe
RUN cd cmd/warp-pipe && go install .
