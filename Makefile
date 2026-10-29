# test: run the lambda tests and the Floci end-to-end test in containers, then tear the stack down.
# up:   start Floci with the pipeline deployed, so PDFs can be uploaded by hand; Ctrl-C tears it down.
COMPOSE      := docker compose -f compose.test.yml
COMPOSE_UP   := docker compose -f compose.test.yml -f compose.up.yml

.PHONY: test up down

test:
	@trap '$(COMPOSE) down -v --remove-orphans' EXIT; trap 'exit 130' INT TERM HUP; \
	$(COMPOSE) up --build --abort-on-container-exit --exit-code-from tests

up:
	@trap '$(COMPOSE_UP) down -v --remove-orphans' EXIT; trap 'exit 130' INT TERM HUP; \
	$(COMPOSE_UP) up --build --abort-on-container-exit --exit-code-from dev dev pdf-floci

# Manual teardown if `make up` was killed hard.
down:
	$(COMPOSE_UP) down -v --remove-orphans
