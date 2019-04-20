FROM golang:alpine AS build
COPY . /go/src/github.com/perangel/warp-pipe
WORKDIR /go/src/github.com/perangel/warp-pipe
RUN cd cmd/warp-pipe && go install .

FROM alpine AS release
COPY --from=build /go/bin/warp-pipe /usr/bin/warp-pipe
CMD ["warp-pipe"]
