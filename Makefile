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

.PHONY: integration-test
integration-test:
	go test -v ./tests/integration -integration
