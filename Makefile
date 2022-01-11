VERSION ?= $(shell git rev-parse --short HEAD)

.PHONY: all
all:
	@cd cmd/warp-pipe && go build -v

.PHONY: install
install:
	@cd cmd/warp-pipe && go install

.PHONY: demo
demo:
	./scripts/setup_demo.sh

.PHONY: demo-clean
demo-clean:
	docker-compose -f docker-compose.demo.yml rm -f -s

.PHONY: lint
lint:
	@go run vendor/github.com/golangci/golangci-lint/cmd/golangci-lint/main.go -v run

.PHONY: test
test:
	go test -v ./...

.PHONY: integration-test
integration-test:
	docker build -f ./build/postgres/Dockerfile -t psql-int-test:$(VERSION) .
	BUILD_SHA=$(VERSION) go test -v ./tests/integration -integration
