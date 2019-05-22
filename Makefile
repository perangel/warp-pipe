.PHONY: all
all: 
	cd cmd/warp-pipe && go build -v

.PHONY: demo
demo: demo-clean
	./scripts/setup_demo.sh

.PHONY: demo-clean
demo-clean:
	docker-compose -f docker-compose.demo.yml rm -s
