# Evaluation section template

## Experimental environment

We evaluated the Rebuild-First CDC Readiness Model in a containerized GitHub Codespaces testbed. The source was Oracle Database Free, and committed changes from the `RETAIL.ORDERS` table were captured with the Debezium Oracle connector using LogMiner. Kafka retained the change stream. A Java projection consumer applied each event to a PostgreSQL relational projection and an Apache Ignite low-latency cache. The workload generator produced inserts, updates, and physical deletes using synthetic retail-order data. The measurements are controlled-testbed results and are not presented as production benchmarks.

## Evaluation procedure

The evaluation covered five scenarios: normal operation, Kafka Connect restart, PostgreSQL unavailability, duplicate replay, and complete target reconstruction. After each scenario, the sandbox compared the active Oracle rows with the PostgreSQL projection by ordered row count and SHA-256 digest. It also compared the source count with the active Ignite cache count. The sink recorded processed records, applied records, ignored duplicate or stale records, processing failures, and end-to-end latency percentiles.

## Results

Insert the generated values from `results/evaluation_summary.csv` here. Report the environment size and workload rate alongside the results. Do not generalize the measurements as enterprise-scale performance results.

Suggested table:

| Scenario | Source rows | PostgreSQL rows | Ignite rows | P50 latency | P95 latency | Duplicate/stale records | Reconciliation |
|---|---:|---:|---:|---:|---:|---:|---|
| Normal workload | | | | | | | |
| Connector restart | | | | | | | |
| Target outage | | | | | | | |
| Duplicate replay | | | | | | | |
| Complete rebuild | | | | | | | |

## Interpretation

Discuss which controls were demonstrated by each scenario. The strongest evidence is the complete-rebuild experiment: both target projections are cleared, the consumer group is reset to the beginning of the retained topic, and the projections are reconstructed without a full source-table reload. Successful count and digest reconciliation provide evidence of rebuild correctness for the tested dataset.

## Threats to validity

The sandbox uses synthetic data, one Oracle source table, a single Kafka broker, one PostgreSQL instance, and one Ignite node. The results establish functional recovery behavior in the evaluated configuration, not enterprise throughput, availability, cost, or production-scale source-load reduction. Additional testing would be required before applying the numerical results to a production environment.
