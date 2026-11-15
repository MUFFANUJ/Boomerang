# Boomerang — common tasks. Docker Desktop must be running.
DC := docker compose

.PHONY: up down reset logs ps obs test test-integration chaos loadtest web-dev

up:            ## Build and start the platform (dashboard on http://localhost:8080)
	$(DC) up -d --build

down:          ## Stop everything (keeps data)
	$(DC) --profile observability down

reset:         ## Stop everything and delete all data (fresh demo)
	$(DC) --profile observability down -v

obs:           ## Also start Prometheus (:9090), Grafana (:3000), Loki and Jaeger (:16686)
	OTEL_ENDPOINT=http://jaeger:4318 $(DC) --profile observability up -d

logs:          ## Tail backend logs
	$(DC) logs -f --tail=50 api fanout worker retry recorder realtime

ps:
	$(DC) ps

test:          ## Unit tests
	cd backend && go test -race ./...

test-integration: ## Integration tests against real Kafka/Redis/Postgres (Testcontainers)
	cd backend && go test -tags integration -count=1 ./internal/integration/...

chaos:         ## SIGKILL a worker mid-burst and prove no event is lost
	./scripts/chaos-kill-worker.sh 20000 3

loadtest:      ## k6 ramp to 2,000 events/s through the gateway
	docker run --rm -i --network boomerang_default -e BASE_URL=http://gateway -e PEAK_RATE=2000 \
		grafana/k6:0.55.0 run - < loadtest/k6/ingest.js

web-dev:       ## Dashboard dev server with hot reload on :5173 (proxies to :8080)
	cd web && npm install && npm run dev
