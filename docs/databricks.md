# Running on Databricks

The jobs are plain PySpark and Delta, so they run on Databricks without code
changes. Only configuration moves.

## Tables and checkpoints in Unity Catalog

All paths come from `Settings` in `src/plant_telemetry/common/config.py`, built
from `LAKE_ROOT`. Point it at a Unity Catalog volume:

```
LAKE_ROOT=/Volumes/plant/telemetry/lake
```

Bronze, silver, gold and the `_checkpoints/` folder then live under that
volume. To register the tables as managed Unity Catalog tables instead of
paths, swap the `.save(path)` / `.load(path)` calls for `.toTable(name)` /
`spark.read.table(name)` and give each stream its own checkpoint folder in a
volume. Keep one checkpoint per query: two queries sharing one will corrupt
each other's offsets.

`get_spark()` is only for local runs. On Databricks the cluster already has
Delta and the Kafka connector, so the entry points can take the existing
`spark` session from the job context.

## Managed Kafka instead of Redpanda

Set `KAFKA_BOOTSTRAP_SERVERS` to the managed endpoint and add the security
options to `read_kafka()` in `pipelines/bronze.py`, for example for SASL/SSL:

```python
.option("kafka.security.protocol", "SASL_SSL")
.option("kafka.sasl.mechanism", "PLAIN")
.option("kafka.sasl.jaas.config", jaas_config_from_secret_scope)
```

Read credentials from a Databricks secret scope (`dbutils.secrets.get`), never
from code or job parameters.

## Job layout

A reasonable job has three tasks:

1. `bronze`: the two Kafka streams, continuous or on a short trigger.
2. `silver`: telemetry and work-order streams reading bronze Delta.
3. `gold`: `run_gold()` on a schedule (for example every 15 minutes), since it
   recomputes a short look-back window with `replaceWhere`.

`pipelines/run.py --mode continuous` is the single-process equivalent for
local use.
