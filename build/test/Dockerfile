FROM golang:1.17-alpine3.15 AS build
COPY . /go/src/github.com/perangel/warp-pipe
WORKDIR /go/src/github.com/perangel/warp-pipe
RUN cd cmd/warp-pipe && go install .
