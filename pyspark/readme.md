# PySpark for Data Engineering — Exhaustive Topic Guide

> A practical reference covering every topic companies expect data engineers to know.

***

## 1. Core Spark Foundations

- Apache Spark overview — when to use Spark vs Pandas/SQL-only pipelines, distributed data processing fundamentals
- Spark architecture — driver, executors, cluster manager, worker nodes, application lifecycle
- Cluster managers and deployment modes — Standalone, YARN, Kubernetes, Databricks-style managed platforms
- SparkSession, SparkContext, basic application structure, `pyspark` shell, `spark-submit`
- Jobs, stages, tasks, DAGs, lineage, lazy evaluation, actions vs transformations
- Narrow vs wide transformations, shuffle mechanics, partition boundaries
- RDD fundamentals — when RDDs matter, why DataFrames are preferred, RDD vs DataFrame vs Dataset concepts
- DataFrame fundamentals — schema-based processing, columnar thinking, immutable transformations
- Spark SQL fundamentals — temporary views, SQL queries from PySpark, DataFrame API vs SQL API
- Spark configuration basics — executor memory, cores, shuffle partitions, serialization-related settings

***

## 2. Data Ingestion

- Reading data — CSV, JSON, Parquet, ORC, Avro, text, Hive tables, JDBC sources
- Writing data — save modes (overwrite, append, ignore, errorIfExists), partitioned writes, file layout strategy
- Working with object stores and distributed storage — S3, HDFS, ADLS, GCS basics
- Schema definition and enforcement — explicit schema vs inference, nullable fields, schema drift awareness
- JDBC connectivity — reading from and writing to relational databases
- Reading from Kafka and event-based sources
- Handling compressed files and multi-file datasets

***

## 3. DataFrame API & Transformations

- Basic DataFrame operations — `select`, `filter`, `where`, `withColumn`, `drop`, `distinct`, `alias`, `limit`
- Column expressions — `expr`, `lit`, `col`, `when/otherwise`, casting, conditional derivations
- Data types — primitive types, arrays, maps, structs, nested columns
- Null handling — missing values, default fills, imputation patterns, null-safe logic
- String functions — `trim`, `lower`, `upper`, `regexp_replace`, `split`, `substring`, `concat`, `like`
- Date and timestamp functions — `to_date`, `to_timestamp`, `date_diff`, `date_add`, `date_trunc`, `year`, `month`, `dayofweek`
- Numeric functions — `round`, `ceil`, `floor`, `abs`, `log`, `pow`, `percentile_approx`
- Array functions — `array_contains`, `size`, `flatten`, `array_distinct`, `sort_array`, `zip_with`
- Map functions — `map_keys`, `map_values`, `map_filter`, `explode` on maps
- Aggregations — `groupBy`, `agg`, count, count distinct, min, max, avg, sum, approximate aggregations
- Joins — inner, left, right, full, semi, anti, cross, multi-key joins, join conditions, duplicate-column handling
- Union patterns — `union`, `unionByName`, schema alignment across datasets
- Sorting and ranking — `orderBy`, `sortWithinPartitions`, top-N problems
- Window functions — `row_number`, `rank`, `dense_rank`, `lag`, `lead`, cumulative sums, moving averages, `ntile`
- Pivoting and reshaping — `pivot`, unpivot-style transformations, wide-to-long and long-to-wide patterns
- Deduplication patterns — exact duplicates, business-key deduplication, latest-record selection per key
- Working with nested and semi-structured data — flattening JSON, `explode`, `explode_outer`, array and map manipulation
- Parsing and generating JSON — nested schema handling, schema evolution awareness for semi-structured sources
- Data cleaning patterns — standardization, trimming, type correction, invalid record handling
- ETL design patterns — extract-transform-load flow, staging layers, reusable transformation modules, idempotent processing

***

## 4. Spark SQL

- SQL on DataFrames — `spark.sql()`, temp views, global temp views
- DDL operations — `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE` via Spark SQL
- DML operations — `INSERT INTO`, `INSERT OVERWRITE`, `MERGE INTO` (Delta/Lakehouse)
- Hive metastore integration — managed vs external tables, catalog awareness
- Subqueries — scalar subqueries, correlated subqueries, `IN`, `EXISTS`
- CTEs (Common Table Expressions) with `WITH` clause
- Analytical SQL — window functions, ranking, running totals in SQL syntax
- SQL optimization hints — broadcast hints, join hints
- Multi-catalog and namespace handling

***

## 5. Performance & Optimization

- Catalyst optimizer basics — logical plan vs physical plan, how Spark optimizes queries
- Tungsten concepts and execution efficiency awareness
- Execution plans — `explain()`, `explain("extended")`, reading physical plans, spotting shuffles and expensive operators
- Partitioning strategy — repartitioning, coalescing, partition count tuning
- Bucketing and sorting — when they help joins and downstream reads
- Caching and persistence — when to cache, persistence levels (`MEMORY_ONLY`, `MEMORY_AND_DISK`, etc.), cache misuse pitfalls
- Join optimization — broadcast joins (`broadcast()`), shuffle hash joins, sort-merge joins, join order awareness
- Data skew detection and mitigation — skewed keys, salting technique, broadcast alternatives, repartitioning tactics
- Shuffle reduction techniques — pre-aggregation, filter early, projection pruning, narrow transformations
- Predicate pushdown, partition pruning, column pruning, efficient file scans
- File format choices — why Parquet/ORC matter, compression trade-offs (snappy, gzip, zstd), row vs columnar storage
- Small files problem — compaction awareness, output file sizing, `coalesce` for write optimization
- UDF performance trade-offs — Python UDFs vs built-in functions vs Pandas UDFs (vectorized UDFs)
- Memory management basics — executor memory pressure, spill to disk, out-of-memory symptoms, serialization overhead
- Adaptive Query Execution (AQE) — skew join optimization, coalescing post-shuffle partitions, dynamic partition pruning
- Spark UI — stage analysis, task-level metrics, shuffle read/write, skew diagnosis, history server

***

## 6. User-Defined Functions (UDFs)

- Python UDFs — `udf()`, return type declaration, performance caveats
- Vectorized Pandas UDFs — `pandas_udf`, Series to Series, grouped map, grouped aggregate patterns
- UDF registration for SQL usage — `spark.udf.register()`
- When NOT to use UDFs — prefer built-in functions first
- Higher-order functions — `transform`, `filter`, `aggregate`, `forall`, `exists` on arrays (no UDF needed)

***

## 7. Schema & Data Quality Management

- Schema definition — `StructType`, `StructField`, data type imports
- Schema inference vs explicit schema — trade-offs for production pipelines
- Schema evolution handling — `mergeSchema`, `overwriteSchema`
- Schema validation patterns — column existence checks, type checks, null ratio checks
- Data quality rules — not-null assertions, range checks, referential integrity, deduplication checks
- Record count reconciliation — source vs target validation
- Bad records handling — `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST` modes, `_corrupt_record` column
- Dead-letter / quarantine pattern for invalid records

***

## 8. File Formats & Storage

- Parquet — columnar storage, row groups, compression, predicate pushdown, schema evolution
- ORC — optimized row columnar, Hive-native, ACID support
- Avro — row-based, schema registry compatibility, Kafka-friendly
- JSON and CSV — when to use, performance caveats, multiline handling
- Delta Lake — ACID transactions, versioning, time travel, `MERGE`, schema enforcement and evolution, `OPTIMIZE`, `VACUUM`
- Apache Iceberg — open table format, snapshot isolation, hidden partitioning awareness
- Apache Hudi — upsert-optimized table format, COW vs MOR table types awareness
- Compression codecs — Snappy, GZIP, ZSTD, LZ4, Brotli trade-offs
- Partitioned datasets — partition discovery, partition overwrite, dynamic vs static partition writes

***

## 9. Structured Streaming

- Structured Streaming fundamentals — streaming DataFrames, triggers, output modes (`append`, `complete`, `update`)
- Streaming sources — Kafka, file sources, Delta table as source
- Streaming sinks — Kafka, file sinks, Delta tables, JDBC, memory sink (for testing)
- Checkpointing — fault tolerance, recovery, checkpoint location management
- Event-time vs processing-time — `window()`, `tumbling windows`, `sliding windows`
- Watermarks — handling late-arriving data, `withWatermark()`
- Stateful streaming — stateful aggregations, `mapGroupsWithState`, `flatMapGroupsWithState`
- Stream-static joins and stream-stream joins
- Exactly-once and at-least-once processing semantics
- Kafka integration deep dive — consumer groups, offsets, schema registry, Avro/JSON deserialization
- Streaming pipeline monitoring — query progress, streaming metrics

***

## 10. Lakehouse & Modern Data Architecture

- Lakehouse architecture concepts — bronze/silver/gold (medallion architecture) layers
- Delta Lake operations — `MERGE INTO` (upsert), `UPDATE`, `DELETE`, `INSERT`
- Delta time travel — `VERSION AS OF`, `TIMESTAMP AS OF`, audit and rollback
- Delta `OPTIMIZE` and `VACUUM` for storage management
- Delta constraints and column-level metadata
- Schema enforcement and evolution in Delta
- Change Data Capture (CDC) patterns — full load vs incremental load, SCD Type 1 / Type 2 awareness
- Incremental processing patterns — watermark columns, high-watermark strategy
- Slowly Changing Dimensions (SCD) implementation in PySpark

***

## 11. Production Pipeline Engineering

- End-to-end batch pipeline design with PySpark
- Idempotent job design — safe to re-run without data duplication
- Incremental load patterns — partition-based, watermark-based, CDC-based
- Pipeline modularity — reusable transformation functions, config-driven parameterization
- Error handling — `try/except` in PySpark drivers, bad record routing, job failure recovery
- Logging strategy — structured logs, job-level metadata, stage-level observability
- Data lineage awareness — tracking source-to-target field mappings
- Data reconciliation — count checks, sum checks, hash-based validation
- Monitoring and alerting — Spark UI, history server, runtime metrics, integration with monitoring tools
- Secrets and credential management — secure storage connectivity, no hardcoded credentials
- Configuration management — external configs (`configparser`, YAML, environment variables)
- Environment separation — dev/staging/prod pipeline management

***

## 12. Testing & Code Quality

- Unit testing PySpark code — `pytest`, local SparkSession in tests, fixture patterns
- Data assertion libraries — `chispa` for DataFrame equality checks
- Schema validation tests — assert expected vs actual schema
- Transformation-level tests — input → expected output assertion patterns
- Integration testing — testing end-to-end pipeline runs against small datasets
- Mocking external sources in tests — mock file paths, JDBC, Kafka
- Code quality standards — PEP 8, type hints, docstrings, `pylint`/`flake8`/`black` awareness
- Logging and observability in tests

***

## 13. Deployment & Orchestration

- `spark-submit` — job submission, config flags, `--py-files`, `--jars`, `--conf`
- Packaging Python projects — `setup.py`, `pyproject.toml`, wheel files, `zip` archives for dependencies
- Dependency management — `requirements.txt`, `conda`, virtual environments, Docker-based packaging
- Cluster deployment modes — client mode vs cluster mode trade-offs
- Databricks job deployment — notebooks, Python wheel jobs, job clusters vs all-purpose clusters
- EMR deployment — step functions, bootstrap scripts, EMR on EC2 and EMR Serverless
- Dataproc deployment — GCP-native Spark cluster management
- Orchestration integration — Apache Airflow (`SparkSubmitOperator`, `DatabricksRunNowOperator`), triggering Spark jobs from DAGs
- CI/CD for data pipelines — GitHub Actions / Jenkins, automated test runs, environment promotion
- Containerized Spark — Docker, Kubernetes deployment, Spark-on-K8s operator awareness

***

## 14. Cloud & Platform Integration

- AWS — S3 integration (`s3a://`), Glue catalog, EMR, Redshift via JDBC/Redshift Connector
- Azure — ADLS Gen2 (`abfss://`), Azure Databricks, Synapse Spark pools, Event Hubs as Kafka source
- GCP — GCS (`gs://`), Dataproc, BigQuery Spark connector
- Databricks-specific — Unity Catalog, Databricks Repos, Delta Live Tables (DLT) awareness, Auto Loader
- IAM and access control — role-based access, instance profiles, service principals
- Secrets management — AWS Secrets Manager, Azure Key Vault, Databricks secrets

***

## 15. Advanced & Interview-Prep Topics

### Advanced APIs

- RDD operations — `map`, `flatMap`, `reduceByKey`, `groupByKey`, `mapPartitions`, `zip`, `cogroup`
- Broadcast variables and accumulators — use cases, thread-safety, pitfalls
- Custom partitioners (RDD level)
- Pandas API on Spark (formerly Koalas) — pandas-compatible API on distributed data

### Common Coding Problems Companies Expect

- Deduplicate records keeping the latest by timestamp per business key
- Top-N records per group using window functions
- Running totals and cumulative metrics
- Year-over-year / month-over-month comparisons using `lag`
- Flatten deeply nested JSON with arrays of structs
- SCD Type 2 implementation using `MERGE`
- Handling multi-source joins with schema differences
- Broadcast join vs shuffle join — when and why
- Salting skewed keys in a join
- Partitioned write with dynamic partition overwrite
- Group-wise missing value imputation
- Streaming word count with windowed aggregation

### Scenario-Based Troubleshooting Companies Expect

- Job runs slowly — how to diagnose (Spark UI, stage skew, shuffle size)
- OOM on executors during a join — root cause and fixes
- Inconsistent schemas across partitions breaking reads
- Skewed join causing one task to run 100× longer than others
- Small files accumulation slowing downstream reads — compaction strategy
- Pipeline produces duplicate records on reruns — idempotency fix
- Late-arriving data causing incorrect streaming aggregations — watermark fix
- Schema evolution breaking existing reads — handling with `mergeSchema`

### System-Level Expectations in Interviews

- Ability to design a full batch ETL pipeline from ingestion to serving
- Streaming pipeline design with Kafka + Spark + Delta
- Medallion architecture design for a given business scenario
- Choosing between batch and streaming for a given SLA
- Explaining Spark internals: shuffle, DAG, catalyst optimizer, AQE
- Trade-offs between Delta Lake, Iceberg, and Hudi
- Cloud-native pipeline design on AWS/Azure/GCP
- Data quality framework design within a Spark pipeline
- CI/CD and testing strategy for production Spark jobs

***
