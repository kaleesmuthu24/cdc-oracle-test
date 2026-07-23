.PHONY: bootstrap evaluate status workload reconcile logs clean

bootstrap:
	./scripts/bootstrap.sh

evaluate:
	./scripts/run-evaluation.sh

status:
	./scripts/status.sh

workload:
	python tools/workload.py run --events 1000 --rate 50

reconcile:
	python tools/reconcile.py --wait 300

logs:
	docker compose logs -f --tail=100

clean:
	./scripts/clean.sh
