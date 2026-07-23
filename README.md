# Rebuild-First CDC Evaluation Sandbox

This repository provides a reproducible GitHub Codespaces testbed for evaluating a **Rebuild-First Change Data Capture readiness model** without using confidential production measurements.

## What it runs

- Oracle Database Free as the transactional source.
- Debezium Oracle Connector with LogMiner.
- Apache Kafka in KRaft mode.
- PostgreSQL as a durable relational projection.
- Apache Ignite as a low-latency key-value projection.
- A Java Kafka consumer implementing idempotent, SCN-ordered projection updates.
- Python workload, reconciliation, and evidence-generation utilities.

The sandbox uses a synthetic `RETAIL.ORDERS` table. It generates actual testbed measurements; it does not contain invented benchmark results.

## Codespaces requirement

Choose at least a **4-core / 16-GB** Codespace. Oracle, Kafka Connect, Kafka, PostgreSQL, Ignite, and the sink application run simultaneously. A smaller machine may be unstable or very slow.

## Start in GitHub Codespaces

1. Create a new GitHub repository.
2. Upload the contents of this project to the repository root.
3. Select **Code → Codespaces → Create codespace**.
4. Choose a 4-core or larger machine when prompted.
5. After the dev container finishes, run:

```bash
make bootstrap
```

The first startup pulls several large images and initializes Oracle. The script waits for the services, seeds the source table, registers Debezium, and verifies the initial projection.

Check the environment:

```bash
make status
```

## Run the evaluation

```bash
make evaluate
```

The evaluation runs:

1. Normal continuous CDC.
2. Kafka Connect restart while source transactions continue.
3. PostgreSQL outage and catch-up.
4. Duplicate replay from the beginning of the Kafka topic.
5. Complete PostgreSQL and Ignite reconstruction from retained events.

Generated evidence is written to `results/`:

```text
results/
  evaluation_summary.csv
  normal.json
  connector_restart.json
  target_outage.json
  duplicate_replay.json
  complete_rebuild.json
  readiness_score.csv
```

## Run an additional workload

```bash
python tools/workload.py run --events 5000 --rate 100 --seed 900
python tools/reconcile.py --wait 600
```

The requested rate is a workload target, not a guaranteed rate. Oracle commit time and Codespaces resources determine the achieved rate.

## Useful endpoints

- Kafka Connect status: `http://localhost:8083/connectors/oracle-retail-orders/status`
- Sink health: `http://localhost:18080/health`
- Sink metrics: `http://localhost:18080/metrics`

Codespaces forwards these ports automatically.

## Rebuild mechanics

The sink stores the Oracle SCN with each target record. A target write is accepted only when its SCN is newer than the current target SCN. Physical deletes are retained as target tombstones rather than being removed from the state table. This protects the projection from stale records during replay.

For a complete rebuild, the evaluation script:

1. Clears the PostgreSQL and Ignite projections.
2. Stops the sink consumer.
3. Resets its Kafka consumer-group offsets to the beginning.
4. Restarts the sink.
5. Waits for replay to finish.
6. Compares source and target counts and ordered SHA-256 row digests.

## Paper wording

Use `paper/evaluation-section-template.md` as the starting point for the revised manuscript. Clearly describe the figures as **controlled synthetic-testbed results**, not production benchmarks.

## Troubleshooting

### Oracle initialization fails

Review:

```bash
docker logs cdc-oracle --tail 200
```

Reset all persistent data and retry:

```bash
make clean
make bootstrap
```

### Connector fails

Review its status and logs:

```bash
curl -s http://localhost:8083/connectors/oracle-retail-orders/status | python -m json.tool
docker logs cdc-connect --tail 200
```

### Reconciliation does not converge

Review sink logs and metrics:

```bash
docker logs cdc-sink-app --tail 200
curl -s http://localhost:18080/metrics | python -m json.tool
```

### Stop without deleting data

```bash
docker compose stop
```

### Remove all containers and test data

```bash
make clean
```

## Evaluation boundaries

This sandbox can support claims about replay correctness, idempotency, connector restart, temporary target outage, reconstruction, and reconciliation for the tested configuration. It cannot support claims about billion-record scale, enterprise availability, actual production source-load reduction, cost savings, or organization-wide reliability.
# cdc-oracle
