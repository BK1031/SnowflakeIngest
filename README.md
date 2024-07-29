# Snowflake Ingestion Benchmarks

Snowflake has a couple recommended methods for data ingestion. Below are a few quick benchmarks across the various methods. See `snowflake.sql` for the required setup like creating a database, schema, warehouse, user, and roles. All benchmarks were conducted using an `XS` warehouse (8 cores, 16GB RAM).

## SQL Insertion

The first ingestion method is through directly running SQL Insert statements. While this is a documented method, they do acknowledge that it is not efficient since Snowflake is an OLAP engine optimized for writing large batches of data.

The benchmark for this was 1,000 rows of randomly generated data being sequentially inserted. Then a separate thread would query the database until the row was created. The difference between the send time and the receive time was noted as the latency for that insertion.

```
Inserted 1,000 records in 504707.83 ms
+-----------------------+---------------+
| Metric                | Latency (ms)  |
+-----------------------+---------------+
| Average latency       |        664.46 |
| Minimum latency       |        429.71 |
| Maximum latency       |       2688.53 |
| 95th percentile       |        861.39 |
| 99th percentile       |       1143.23 |
+-----------------------+---------------+
```

## SQL Insertion (concurrent)

The next ingestion method is the same as previous but now with data being sent concurrently. The only code change is now running each insert statement in its own thread. Another thing that is not documented but a bunch of forum posts and articles exist about is the max concurrent write limit of 20. Snowflake tables have a locking mechanism at the table level and not the row level. So even if inserts are executed concurrently, they will actually be queued and once 20 statements have been added to the queue, all further statements will be dropped. This limit applies to any COPY, INSERT, MERGE, UPDATE, or DELETE statements on any table.

The benchmark here was another 1,000 rows being inserted. Because of the queuing nature, concurrent inserts offer a negligible latency improvement to the sequential inserts.

```
Inserted 1,000 records in 495663.01 ms
+-----------------------+---------------+
| Metric                | Latency (ms)  |
+-----------------------+---------------+
| Average latency       |        651.12 |
| Minimum latency       |        414.27 |
| Maximum latency       |       2587.81 |
| 95th percentile       |        837.54 |
| 99th percentile       |       1379.22 |
+-----------------------+---------------+
```

## File Upload & Copy

The next method involves batching multiple rows into a parquet file, uploading that file to Snowflake, and then running a COPY statement to transfer the rows from the file to a database table.

There are two benchmarks for this method. The first one was sending 1,000 rows 100 times.

```
Inserted 100 x 1,000 records in 194131.58 ms
+-----------------------+---------------+
| Metric                | Latency (ms)  |
+-----------------------+---------------+
| Average latency       |       2075.94 |
| Minimum latency       |       1554.80 |
| Maximum latency       |       3385.27 |
| 95th percentile       |       2440.60 |
| 99th percentile       |       3385.27 |
+-----------------------+---------------+
```

The second benchmark was sending 1,000,000 rows 100 times.

```
Inserted 100 x 1,000,000 records in 3134059.94 ms
+-----------------------+---------------+
| Metric                | Latency (ms)  |
+-----------------------+---------------+
| Average latency       |      35991.24 |
| Minimum latency       |      30350.38 |
| Maximum latency       |      42507.33 |
| 95th percentile       |      39438.99 |
| 99th percentile       |      42507.33 |
+-----------------------+---------------+
```

## Snowpipe

The next method is similar to the previous one as we are still batching rows into a parquet file and uploading that file to Snowflake. But instead of uploading to a stage, we are directly uploading to a pipe and Snowflake is taking care of copying data to our table asynchronously. According to Snowflake, this method is one of the most efficient ways to ingest data at high throughputs. Obviously this would depend on the batch size.

Again, there are two benchmarks for this method. The first one being sending 1,000 rows 100 times.

```
Inserted 100 x 1,000 records in 2094131.58 ms
+-----------------------+---------------+
| Metric                | Latency (ms)  |
+-----------------------+---------------+
| Average latency       |      21075.17 |
| Minimum latency       |      14554.52 |
| Maximum latency       |      35385.84 |
| 95th percentile       |      21440.91 |
| 99th percentile       |      34985.63 |
+-----------------------+---------------+
```

The second benchmark was sending 1,000,000 rows 100 times.

```
Inserted 100 x 1,000,000 records in 29134059.94 ms
+-----------------------+---------------+
| Metric                | Latency (ms)  |
+-----------------------+---------------+
| Average latency       |     348991.14 |
| Minimum latency       |     323350.72 |
| Maximum latency       |     426197.45 |
| 95th percentile       |     394328.83 |
| 99th percentile       |     425197.58 |
+-----------------------+---------------+
```

## Kafka Streaming

This method is the recommended method for ingesting data in real-time. Using the Kafka connector with Snowpipe Streaming, messages are flushed directly from the Kafka connector to the Snowpipe Streaming API, which then writes rows of data to a table. Some important things to note are the buffer flush time for Snowpipe Streaming which is 1 second by default and the Kafka Connector buffer flush time, which can be set to 1 second at the lowest. This means that after the Kafka buffer flush time is reached, data will be sent with one second latency to Snowflake through Snowpipe Streaming.

The benchmark for this consists of sending 1,000 messages to kafka, and waiting for them to be ingested into Snowflake, repeated 100 times.

```
+-----------------------+---------------+
| Metric                | Latency (ms)  |
+-----------------------+---------------+
| Average latency       |      56420.47 |
| Minimum latency       |       6330.09 |
| Maximum latency       |      58643.57 |
| 95th percentile       |      57617.57 |
| 99th percentile       |      58643.57 |
+-----------------------+---------------+
```