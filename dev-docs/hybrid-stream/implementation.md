# HybridStream BigQuery Integration — Implementation Guide

This document is a self-contained spec for the HybridStream feature added to the Dataproc fork of
Apache Iceberg. It is detailed enough that another engineer (or agent) can rebuild the entire
implementation **without reading the underlying commits**. It covers the problem, the architectural
decision, and exact file-by-file changes for Slices 1–3 (data model, driver-side planning,
executor-side reader), plus the Spark 4.0 port. Slice 4 (credential vending, file-half routing
refinement, end-to-end testing) is sketched at the end as the immediate next-step; its discovery
brief — for an internal Google research agent — lives next to this file at
[`slice-4-research-prompt.md`](slice-4-research-prompt.md).

> **Status:**
> - Iceberg base: 1.10 line, branch tag `apache-iceberg-1.10.0-970-g5dae5fc85`
> - Spark targets: **3.5 (Scala 2.12)** and **4.0 (Scala 2.13)** — both modules carry the full
>   HybridStream stack; the v4.0 port is a verbatim copy of v3.5 (see [§9 Porting](#9-porting-to-additional-spark-versions)).
> - `spark-bigquery-connector`: 0.42.4, lean libs only (Maven Central)
> - Slices 1, 2, 3 implemented and pushed to feature branches on the fork.
> - Slice 4 not yet started; most of its open questions need google3 / artifact-registry /
>   internal-docs access and are tracked in [`slice-4-research-prompt.md`](slice-4-research-prompt.md).

## Table of contents

1. [Problem & motivation](#1-problem--motivation)
2. [How Iceberg + Spark read today](#2-how-iceberg--spark-read-today)
3. [Architectural choice: Physical Union at the Iceberg executor](#3-architectural-choice-physical-union-at-the-iceberg-executor)
4. [Implementation roadmap (slice overview)](#4-implementation-roadmap-slice-overview)
5. [Slice 1 — Data model: `BqStreamScanTask`](#5-slice-1--data-model-bqstreamscantask)
6. [Slice 2 — Driver-side heterogeneous planning](#6-slice-2--driver-side-heterogeneous-planning)
7. [Slice 3 — Executor-side router + BigQuery reader](#7-slice-3--executor-side-router--bigquery-reader)
8. [Build configuration changes](#8-build-configuration-changes)
9. [Porting to additional Spark versions](#9-porting-to-additional-spark-versions)
10. [Verification commands](#10-verification-commands)
11. [Pitfalls encountered (and how to avoid them)](#11-pitfalls-encountered-and-how-to-avoid-them)
12. [Known limitations and Slice 4 roadmap](#12-known-limitations-and-slice-4-roadmap)
13. [Appendix A — External types we depend on](#appendix-a--external-types-we-depend-on)
14. [Appendix B — Iceberg internals referenced](#appendix-b--iceberg-internals-referenced)

---

## 1. Problem & motivation

### What goes wrong today

Dataproc Spark+Iceberg queries against BigQuery Managed Iceberg tables (a.k.a. BigLake/Iceberg
tables) **read incomplete data**. A Managed Iceberg table can keep some rows in raw GCS Parquet
files (the historical/cold half) and others in BigQuery's Vortex storage (the recent/hot half,
real-time-ingested or under fine-grained access control).

- Iceberg's standard reader path is purely file-oriented (`FileIO` → `InputFile` → vectorized
  Parquet/ORC reader). It can read the GCS half but is *blind* to anything in Vortex.
- The reverse path through `spark-bigquery-connector` reads everything correctly via the BigQuery
  Storage Read API, but it routes **all** data — including the large historical GCS half —
  through gRPC streams. This is functional but throughputs poorly: gRPC + Arrow round-trips are
  much more expensive per byte than seek/scan over Parquet on GCS.

### What we want

A single Iceberg scan that emits **both** kinds of work in one plan:

- For the GCS half: standard `FileScanTask`s, read by Iceberg's existing vectorized Parquet/ORC
  path.
- For the Vortex / FGAC half: a new task type that points at a BigQuery Storage Read API stream
  ID, read by a gRPC-based Arrow → `ColumnarBatch` reader.

### What the BigQuery side gives us

A new BigQuery API, `GenerateScanPlan` (RPC method on
`com.google.cloud.bigquery.storage.v1beta2.BigQueryRead`), takes a query and returns a single
"HybridStream" response containing **both** halves:

- `gcs_file_scan_tasks`: GCS URIs + offsets + lengths for raw Parquet/Avro files.
- `bigquery_read_session`: a BigQuery Storage Read API `ReadSession` with stream IDs that yield
  Arrow record batches over gRPC.

### Goals

- Drastically reduce planning latency: one gRPC call replaces metadata reads + file listing.
- Maintain throughput on the historical half: Iceberg's vectorized Parquet path stays untouched.
- Add the streaming half: use the BigQuery Storage Read API for Vortex/FGAC rows, with Arrow
  zero-copy into Spark's Tungsten engine.
- **Do not** force the streaming half through Iceberg's `FileIO`. That would mean
  re-serializing pre-parsed Arrow rows back to bytes and re-parsing them — a CPU/memory black
  hole. `FileIO` is also engine-agnostic (`iceberg-core`); a Spark+BigQuery-specific shim there
  is a layering violation.

### Non-goals (for the slices documented here)

- No Iceberg 1.11 REST `PlanTable` integration (it doesn't exist yet in this base).
- No Native Query Engine (Velox / NQE) support — that's a separate path.
- No write side (HybridStream is read-only in this design).

---

## 2. How Iceberg + Spark read today

### 2.1 Driver-side: planning

The relevant call chain when Spark queries an Iceberg table (Spark 3.5, DataSourceV2):

1. **`SparkScanBuilder`** at
   [`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkScanBuilder.java`](../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkScanBuilder.java).
   Implements `ScanBuilder` and the various `SupportsPushDown*` mixins. Its `build()` calls
   `buildBatchScan()` (line ~409 today) which constructs a `SparkBatchQueryScan`.

2. **`SparkBatchQueryScan extends SparkPartitioningAwareScan<PartitionScanTask> extends SparkScan`** at
   [`SparkBatchQueryScan.java`](../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java),
   [`SparkPartitioningAwareScan.java`](../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkPartitioningAwareScan.java),
   [`SparkScan.java`](../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkScan.java).
   `SparkPartitioningAwareScan.tasks()` calls `scan.planFiles()` on the wrapped Iceberg
   `Scan<?, FileScanTask, ?>` and validates each task is the configured `taskJavaClass()` —
   so the existing surface is **homogeneous by design**.

3. **`SparkScan.toBatch()`** returns a `SparkBatch` constructed with the planned task groups.

4. **`SparkBatch`** at
   [`SparkBatch.java`](../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatch.java)
   implements Spark's `Batch`. Two key methods:
   - `planInputPartitions()` (line ~87): wraps each `ScanTaskGroup` in a `SparkInputPartition`,
     broadcasting the `Table` and `FileIO`.
   - `createReaderFactory()` (line ~130): inspects all task groups. If every group is a
     Parquet `FileScanTask` group → `SparkColumnarReaderFactory(parquetConf)`; if ORC →
     `SparkColumnarReaderFactory(orcConf)`; otherwise falls back to `SparkRowReaderFactory()`.
     Crucially this is **one factory for the whole batch**.

5. **`SparkInputPartition`** at
   [`SparkInputPartition.java`](../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkInputPartition.java)
   already exposes `<T extends ScanTask> boolean allTasksOfType(Class<T> javaClass)` — this
   helper is reused on the executor side for our routing.

### 2.2 Executor-side: reading

1. Spark calls the broadcast `PartitionReaderFactory`'s `createColumnarReader` (or
   `createReader` for row mode) with each `SparkInputPartition`.

2. The factory chooses a concrete reader. For columnar Parquet:
   `BatchDataReader extends BaseBatchReader<FileScanTask>` at
   [`BatchDataReader.java`](../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/BatchDataReader.java).
   For row-based reads, dispatch happens by task type at
   [`SparkRowReaderFactory.java`](../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkRowReaderFactory.java)
   — there's prior art for "different task types → different reader implementations" within
   one factory (`FileScanTask` → `RowDataReader`, `ChangelogScanTask` → `ChangelogRowReader`,
   `PositionDeletesScanTask` → `PositionDeletesRowReader`).

3. Each reader iterates the task group, opens an `InputFile` via `FileIO`, and produces
   `InternalRow` or `ColumnarBatch`.

### 2.3 Why this can't accommodate gRPC streams as-is

- `FileIO` returns a *seekable byte stream*. The BigQuery Storage Read API returns
  *pre-parsed Arrow record batches* over gRPC. Wrapping a gRPC stream in `InputFile` would
  require serializing parsed columns back to bytes so Iceberg's vectorized Parquet reader can
  re-parse them. CPU and memory cost is unacceptable.
- `FileIO` is in `iceberg-core` (engine-agnostic). Putting BigQuery-specific gRPC logic there
  breaks the layering principle that file-system abstractions live below engine integrations.
- `SparkBatch.createReaderFactory()` chooses one factory for the entire batch. With a
  heterogeneous task list, the existing predicates (`useParquetBatchReads`, `useOrcBatchReads`)
  would all return false and fall through to the row-based reader — which would then fail at
  runtime when handed an unknown task type.

---

## 3. Architectural choice: Physical Union at the Iceberg executor

### 3.1 The chosen design

Run the union at the **Iceberg-Spark adapter layer**, not at Spark Catalyst, and not in
`iceberg-core`/`FileIO`:

- **Driver:** the planner emits one heterogeneous `List<ScanTask>` containing both
  `FileScanTask` and a new `BqStreamScanTask`. Bin-packing keeps each `ScanTaskGroup`
  homogeneous (all tasks in one group share the same task type).
- **Executor:** a router `PartitionReaderFactory` dispatches each partition to a file-half
  reader (existing Iceberg path) or a BigQuery-stream reader (new) based on the
  partition's task type.

This mirrors a pattern already in Iceberg: `ChangelogScanTask` coexists with `FileScanTask`
under the umbrella `ScanTask` interface in the api module, and `SparkRowReaderFactory` already
dispatches by task type. We extend that pattern with a new task type.

### 3.2 Alternatives considered and rejected

- **Catalyst-level Logical Union (Spark plan rewrite):** intercept the query and rewrite into
  a `UnionExec(IcebergScan, BigQueryScan)`. Rejected — breaks DSv2's "one Scan per Relation"
  contract, destroys statistics for join planning, and forces every pushdown (filters,
  projections, aggregates) to be implemented twice.
- **Force the gRPC stream through `FileIO`:** wrap the Arrow stream in a custom `InputFile`.
  Rejected — double serialization overhead, and `FileIO` lives at the engine-agnostic level
  (`iceberg-core`), so Spark+BigQuery glue there is a layering violation.
- **Add a method to api `Scan` to emit heterogeneous tasks:** the `Scan` interface is already
  generic on a single `T extends ScanTask`, and `Scan.planTasks()` already exists (returning
  *task groups*, not raw tasks). Retrofitting heterogeneity here is a much bigger api break
  than placing the heterogeneity at the Spark adapter layer. We chose the smaller change.

### 3.3 Architecture diagram

```
DRIVER

  SparkScanBuilder.buildBatchScan()
    │
    ├── if (table is bq-advanced)   <-- Slice 4 dispatch (TODO comment in Slice 2)
    │     new HybridSparkScan(...)
    │       ├─ holds: a regular Iceberg Scan<?, FileScanTask, ?>  (file half)
    │       │         a BqAdvancedScanPlanner SPI                  (stream half)
    │       │         a HybridReaderConfig (executor-side connector inputs)
    │       │
    │       └─ taskGroups()    : merge two homogeneous bin-packed lists
    │          toBatch()       : returns HybridSparkBatch
    │
    └── else
          new SparkBatchQueryScan(...)   <-- existing path, untouched

  HybridSparkBatch.createReaderFactory()
    └─ HybridColumnarReaderFactory(fileDelegate=SparkColumnarReaderFactory(parquetConf),
                                   readerConfig=HybridReaderConfig)


EXECUTOR

  HybridColumnarReaderFactory.createColumnarReader(partition)
    ├─ if partition.allTasksOfType(BqStreamScanTask.class)
    │     return new BigQueryStreamColumnarReader(partition, readerConfig)
    │       └─ wraps spark-bigquery-connector's ArrowInputPartitionContext
    │
    └─ else
          return fileDelegate.createColumnarReader(partition)   <-- BatchDataReader
```

---

## 4. Implementation roadmap (slice overview)

The work is split into **four** vertical slices plus a porting sub-task. Slices 1–3 and the
v4.0 port are done; Slice 4 is pending.

| Slice | Status | Purpose | Module(s) |
|---|---|---|---|
| 1 | DONE | `BqStreamScanTask` interface + impl + serialization | `iceberg-api`, `iceberg-core` |
| 2 | DONE | `HybridSparkScan` + heterogeneous bin-packing + `BqAdvancedScanPlanner` SPI | `iceberg-spark-3.5_2.12` |
| 3 | DONE | `HybridColumnarReaderFactory` + `BigQueryStreamColumnarReader` | `iceberg-spark-3.5_2.12` |
| v4.0 port | DONE | Mirror of Slices 2 + 3 in the Spark 4.0 module (Slice 1 is shared via api/core) | `iceberg-spark-4.0_2.13` |
| 4 | TODO | Cred vending, file-half ORC/row fallback, end-to-end test, `bq-advanced` table detection | various — see [`slice-4-research-prompt.md`](slice-4-research-prompt.md) |

Each slice should land as its own commit / branch / PR. Slice N+1 depends on N. The v4.0 port
depends on Slice 3 only (file copy + build edits — no source-level changes); see [§9](#9-porting-to-additional-spark-versions).

---

## 5. Slice 1 — Data model: `BqStreamScanTask`

### 5.1 Files

| Path | Action |
|---|---|
| `api/src/main/java/org/apache/iceberg/ScanTask.java` | MODIFY: add `isBqStreamScanTask()` / `asBqStreamScanTask()` defaults |
| `api/src/main/java/org/apache/iceberg/BqStreamScanTask.java` | NEW: public interface |
| `core/src/main/java/org/apache/iceberg/BaseBqStreamScanTask.java` | NEW: concrete impl |
| `api/src/test/java/org/apache/iceberg/TestScanTaskDiscrimination.java` | NEW: tests for `ScanTask` defaults + override |
| `core/src/test/java/org/apache/iceberg/TestBaseBqStreamScanTask.java` | NEW: serialization round-trip + accessors |

### 5.2 Field design — what the task carries

`BqStreamScanTask` represents **one** BigQuery Storage Read API stream. The driver-side planner
will emit one `BqStreamScanTask` per stream returned by `GenerateScanPlan`. Spark's bin-packing
groups N tasks into a partition; the executor opens N gRPC streams (the connector's
`ReadRowsHelper` already supports the multi-stream case in a single `ArrowInputPartitionContext`).

Fields and their reasons:

| Field | Type | Source | Why this shape |
|---|---|---|---|
| `streamName` | `String` | `ReadStream.name` from `GenerateScanPlan` | e.g. `projects/.../sessions/.../streams/0`. Single stream per task. |
| `readSessionName` | `String` | `ReadSession.name` | Diagnostics; reconstructing a minimal `ReadSession` proto on the executor (the connector's `ArrowInputPartitionContext` accesses `getReadSession().getName()` indirectly via its metrics layer). |
| `serializedArrowSchema` | `byte[]` | `ReadSession.getArrowSchema().getSerializedSchema().toByteArray()` | The connector's executor-side reader needs the Arrow IPC schema. Stored as `byte[]` not `com.google.protobuf.ByteString` so `iceberg-api` does not depend on protobuf. |
| `selectedFields` | `List<String>` | Projection from Spark | Passed straight through to the connector's `ArrowInputPartitionContext`. |
| `estimatedRowCount` | `long` | `ReadStream.StreamStats` if available, else default | Drives bin-packing weight. |
| `estimatedSizeBytes` | `long` | `ReadStream.StreamStats` if available, else default | Drives bin-packing weight. |

Deliberate **non-fields** (these are job-level, not task-level — they live in
`HybridReaderConfig` from Slice 3, not on the task):

- `BigQueryClientFactory` (credentials)
- `ReadRowsHelper.Options` (retry counts, parallelism, endpoint)
- Tracing / metrics objects

### 5.3 `ScanTask` modification

Mirror the existing `isFileScanTask()` / `asFileScanTask()` pattern.

```java
// add to api/src/main/java/org/apache/iceberg/ScanTask.java, immediately after the
// asDataTask() block and before the asCombinedScanTask() block

/** Returns true if this is a {@link BqStreamScanTask}, false otherwise. */
default boolean isBqStreamScanTask() {
  return false;
}

/**
 * Returns this cast to {@link BqStreamScanTask} if it is one
 *
 * @return this cast to {@link BqStreamScanTask} if it is one
 * @throws IllegalStateException if this is not a {@link BqStreamScanTask}
 */
default BqStreamScanTask asBqStreamScanTask() {
  throw new IllegalStateException("Not a BqStreamScanTask: " + this);
}
```

### 5.4 `BqStreamScanTask` interface — full source

```java
// api/src/main/java/org/apache/iceberg/BqStreamScanTask.java
//   (Apache 2.0 license header omitted for brevity — copy from any other api/.../*.java file)
package org.apache.iceberg;

import java.util.List;

/**
 * A scan task over a single BigQuery Storage Read API stream.
 *
 * <p>Unlike {@link FileScanTask}, which references a byte range in a file accessed through
 * {@link org.apache.iceberg.io.FileIO}, a {@code BqStreamScanTask} references a server-side gRPC
 * stream produced by the BigQuery {@code GenerateScanPlan} API. The stream returns pre-parsed
 * Arrow record batches; readers consuming this task must therefore bypass {@link
 * org.apache.iceberg.io.FileIO} entirely and connect directly to the BigQuery Storage Read API.
 *
 * <p>A heterogeneous {@link Scan} may produce both {@code FileScanTask} and {@code
 * BqStreamScanTask} instances within a single scan; engines that bin-pack tasks into partitions
 * must keep the two task types in separate partitions so that a single
 * {@link org.apache.spark.sql.connector.read.PartitionReader} (or equivalent) can handle a
 * partition with a single read strategy.
 */
public interface BqStreamScanTask extends ScanTask {
  /** The fully-qualified name of the BigQuery Storage Read API stream. */
  String streamName();

  /** The fully-qualified name of the {@code ReadSession} this stream belongs to. */
  String readSessionName();

  /**
   * The serialized Arrow IPC schema describing the rows produced by this stream.
   *
   * @return a defensive copy of the serialized Arrow schema
   */
  byte[] serializedArrowSchema();

  /** The list of fields projected from the source table, in stream order. */
  List<String> selectedFields();

  @Override
  default boolean isBqStreamScanTask() {
    return true;
  }

  @Override
  default BqStreamScanTask asBqStreamScanTask() {
    return this;
  }
}
```

> **Why not `extend ContentScanTask<...>` or `SplittableScanTask<...>`?**
> `ContentScanTask` is keyed on `ContentFile` (data file or delete file), which BigQuery streams
> are not. `SplittableScanTask` is opt-in — BigQuery streams are pre-split server-side by
> `GenerateScanPlan`, so we should not split them client-side. Closest precedent is the small,
> standalone `ChangelogScanTask` interface ([`api/.../ChangelogScanTask.java`](../../api/src/main/java/org/apache/iceberg/ChangelogScanTask.java)).

### 5.5 `BaseBqStreamScanTask` — full source

The concrete implementation. Uses the same `transient`-cache idiom as `BaseFileScanTask`:
**store fields in a Kryo-friendly form, lazy-build the user-facing immutable view on access.**

```java
// core/src/main/java/org/apache/iceberg/BaseBqStreamScanTask.java
package org.apache.iceberg;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class BaseBqStreamScanTask implements BqStreamScanTask {
  private final String streamName;
  private final String readSessionName;
  private final byte[] serializedArrowSchema;
  private final String[] selectedFields;            // stored as array for Kryo
  private final long estimatedRowCount;
  private final long estimatedSizeBytes;
  private transient volatile List<String> selectedFieldsList = null;  // lazy ImmutableList view

  public BaseBqStreamScanTask(
      String streamName,
      String readSessionName,
      byte[] serializedArrowSchema,
      List<String> selectedFields,
      long estimatedRowCount,
      long estimatedSizeBytes) {
    Preconditions.checkArgument(streamName != null, "Invalid stream name: null");
    Preconditions.checkArgument(readSessionName != null, "Invalid read session name: null");
    Preconditions.checkArgument(
        serializedArrowSchema != null, "Invalid serialized Arrow schema: null");
    Preconditions.checkArgument(selectedFields != null, "Invalid selected fields: null");
    this.streamName = streamName;
    this.readSessionName = readSessionName;
    this.serializedArrowSchema = Arrays.copyOf(serializedArrowSchema, serializedArrowSchema.length);
    this.selectedFields = selectedFields.toArray(new String[0]);
    this.estimatedRowCount = estimatedRowCount;
    this.estimatedSizeBytes = estimatedSizeBytes;
  }

  @Override
  public String streamName() {
    return streamName;
  }

  @Override
  public String readSessionName() {
    return readSessionName;
  }

  @Override
  public byte[] serializedArrowSchema() {
    return Arrays.copyOf(serializedArrowSchema, serializedArrowSchema.length);  // defensive copy
  }

  @Override
  public List<String> selectedFields() {
    if (selectedFieldsList == null) {
      this.selectedFieldsList = ImmutableList.copyOf(selectedFields);
    }
    return selectedFieldsList;
  }

  @Override
  public long estimatedRowsCount() {
    return estimatedRowCount;
  }

  @Override
  public long sizeBytes() {
    return estimatedSizeBytes;
  }
}
```

> **Critical detail (Kryo).** The first attempt stored `selectedFields` directly as an
> `ImmutableList<String>`. Java serialization round-tripped fine, but Spark's Kryo serializer
> failed:
>
> ```
> java.lang.UnsupportedOperationException
>   at org.apache.iceberg.relocated.com.google.common.collect.ImmutableCollection.add(ImmutableCollection.java:266)
>   at com.esotericsoftware.kryo.serializers.CollectionSerializer.read(...)
> ```
>
> Kryo's default `CollectionSerializer` calls `.add()` on the deserialized collection to refill
> it. Iceberg's relocated `ImmutableList.add` throws. The fix is to mirror `BaseFileScanTask`'s
> `DeleteFile[] deletes` field idiom: store as `String[]` and lazy-wrap as `ImmutableList`
> via a `transient` cache (`selectedFieldsList`). Same trick should be applied to any future
> non-primitive Iceberg-relocated collection field on a `ScanTask`.

### 5.6 Tests

#### `TestScanTaskDiscrimination` (api module)

Tests the `ScanTask` defaults + override pattern:

```java
// api/src/test/java/org/apache/iceberg/TestScanTaskDiscrimination.java

@Test
public void defaultScanTaskIsNotBqStreamScanTask() {
  ScanTask task = new ScanTask() {};
  assertThat(task.isBqStreamScanTask()).isFalse();
  assertThat(task.isFileScanTask()).isFalse();
  assertThat(task.isDataTask()).isFalse();
}

@Test
public void defaultAsBqStreamScanTaskThrows() {
  ScanTask task = new ScanTask() {};
  assertThatThrownBy(task::asBqStreamScanTask)
      .isInstanceOf(IllegalStateException.class)
      .hasMessageStartingWith("Not a BqStreamScanTask:");
}

@Test
public void bqStreamScanTaskDiscriminatesAsItself() {
  BqStreamScanTask task = new StubBqStreamScanTask();        // anonymous-class stub
  assertThat(task.isBqStreamScanTask()).isTrue();
  assertThat(task.asBqStreamScanTask()).isSameAs(task);
  assertThat(task.isFileScanTask()).isFalse();
  assertThatThrownBy(task::asFileScanTask)
      .isInstanceOf(IllegalStateException.class)
      .hasMessageStartingWith("Not a FileScanTask:");
}
```

Iceberg's checkstyle requires `assertThatThrownBy` to include a message check
(`hasMessage`/`hasMessageStartingWith`) — a bare `isInstanceOf` will fail with
`"assertThatThrownBy must include a message check"`. Always add one.

#### `TestBaseBqStreamScanTask` (core module)

Tests serialization, accessors, defensive copies, and discrimination. Uses the helpers in
[`api/.../TestHelpers.java`](../../api/src/test/java/org/apache/iceberg/TestHelpers.java) which
expose Java + Kryo round-trip via `@MethodSource("org.apache.iceberg.TestHelpers#serializers")`.

Key cases:

- `accessorsReturnConstructorValues` — every getter returns what was passed in.
- `discriminationReportsBqStreamScanTask` — `isBqStreamScanTask()` true, `isFileScanTask()`
  false, etc.
- `serializedArrowSchemaIsDefensivelyCopied` — mutating the input array after construction
  must not affect the task; mutating the returned array must not affect subsequent reads.
- `rejectsNullArguments` — each constructor `Preconditions.checkArgument` fires.
- `roundTripPreservesAllFields` (`@ParameterizedTest`) — Java + Kryo round-trip preserve all
  fields including the byte[] schema blob.

### 5.7 Verification

```bash
./gradlew :iceberg-api:compileJava :iceberg-core:compileJava
./gradlew :iceberg-api:test --tests TestScanTaskDiscrimination
./gradlew :iceberg-core:test --tests TestBaseBqStreamScanTask
./gradlew :iceberg-api:checkstyleMain :iceberg-api:checkstyleTest \
          :iceberg-core:checkstyleMain :iceberg-core:checkstyleTest \
          :iceberg-api:spotlessJavaCheck :iceberg-core:spotlessJavaCheck
```

All should pass. Run `./gradlew :iceberg-api:spotlessApply :iceberg-core:spotlessApply` if
spotless complains about formatting (line wrapping in particular).

---

## 6. Slice 2 — Driver-side heterogeneous planning

### 6.1 Files

| Path | Action |
|---|---|
| `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/BqAdvancedScanPlanner.java` | NEW: SPI for the BQ stream half |
| `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/HybridSparkScan.java` | NEW: heterogeneous Spark scan |
| `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkScanBuilder.java` | MODIFY: add TODO comment for `bq-advanced` dispatch |
| `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/source/TestHybridSparkScan.java` | NEW: bin-packing homogeneity tests |

### 6.2 Important pivot — keep iceberg-api untouched

A reasonable first instinct is to add a method to `Scan` (in `iceberg-api`) that emits
heterogeneous tasks. **Don't.** Two reasons:

1. `Scan.planTasks()` already exists — it returns balanced `ScanTaskGroup`s, not raw tasks.
   The name is taken.
2. `Scan` is generic on `<T extends ScanTask>` — homogeneous by construction. Forcing
   heterogeneity through the api module is a much larger break than placing it at the Spark
   adapter layer. This is the same layering choice that `SparkRowReaderFactory` makes today
   (it dispatches by task type without any api change).

So: **all heterogeneity lives in the Spark module**. iceberg-api is untouched in Slice 2.

### 6.3 `BqAdvancedScanPlanner` SPI — full source

A small functional interface. The real implementation that calls `GenerateScanPlan` lives in a
downstream module so iceberg-spark-3.5 stays free of BigQuery client deps in Slice 2 (Slice 3
adds those for the executor reader, but the driver-side planning still goes through this SPI).

```java
// spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/BqAdvancedScanPlanner.java
package org.apache.iceberg.spark.source;

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.BqStreamScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;

/**
 * Driver-side planner for the BigQuery Storage Read API stream half of a HybridStream scan.
 *
 * <p>Implementations call the BigQuery {@code GenerateScanPlan} RPC (see {@code
 * com.google.cloud.bigquery.storage.v1beta2.BigQueryRead}) and translate the response into a
 * list of {@link BqStreamScanTask}s. Together with the file-task half produced by Iceberg's
 * standard {@code Scan.planFiles()}, these tasks form the heterogeneous task list consumed by
 * {@link HybridSparkScan}.
 */
public interface BqAdvancedScanPlanner {
  /**
   * Plan the BigQuery Storage Read API streams that contribute to this scan.
   *
   * @param table the Iceberg table being scanned
   * @param projection the projected schema after column pruning
   * @param filters scan filter expressions to push down to {@code GenerateScanPlan}
   * @return the list of stream tasks; never null, may be empty
   */
  List<BqStreamScanTask> planStreams(Table table, Schema projection, List<Expression> filters);

  /** Returns a planner that produces no stream tasks. */
  static BqAdvancedScanPlanner noop() {
    return (table, projection, filters) -> Collections.emptyList();
  }
}
```

### 6.4 `HybridSparkScan` — full source

Notes on the design choices:

- Extends `SparkScan` directly, **not** `SparkPartitioningAwareScan`. The latter implements
  `SupportsReportPartitioning` and assumes every task implements `PartitionScanTask`.
  `BqStreamScanTask` does not — there is no Iceberg partition spec for a BQ stream — and there
  is no shared grouping key across the file and stream halves.
- Stores `spark`, `fileIO`, `readConf` as private fields (in addition to passing them to
  `super(...)`) because `SparkScan` keeps them private and Slice 3 needs to access them in
  `toBatch()` to construct a `HybridSparkBatch`. Modifying `SparkScan` to expose protected
  getters is invasive; the small duplication is contained.
- The `Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>>` generic shape mirrors
  `SparkPartitioningAwareScan`'s constructor.

```java
// spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/HybridSparkScan.java
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.BqStreamScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;

class HybridSparkScan extends SparkScan {

  private final SparkSession spark;
  private final Supplier<FileIO> fileIO;
  private final SparkReadConf readConf;
  private final Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> fileScan;
  private final BqAdvancedScanPlanner bqPlanner;
  private final HybridReaderConfig readerConfig;          // Slice 3 — see Section 7

  // lazy caches
  private List<FileScanTask> fileTasks = null;
  private List<BqStreamScanTask> streamTasks = null;
  private List<ScanTaskGroup<ScanTask>> taskGroups = null;

  HybridSparkScan(
      SparkSession spark,
      Table table,
      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> fileScan,
      BqAdvancedScanPlanner bqPlanner,
      HybridReaderConfig readerConfig,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier) {
    super(
        spark,
        table,
        null != fileScan ? fileScan.fileIO() : table::io,
        readConf,
        expectedSchema,
        filters,
        scanReportSupplier);
    this.spark = spark;
    this.fileIO = null != fileScan ? fileScan.fileIO() : table::io;
    this.readConf = readConf;
    this.fileScan = fileScan;
    this.bqPlanner = bqPlanner != null ? bqPlanner : BqAdvancedScanPlanner.noop();
    this.readerConfig = readerConfig;
  }

  @Override
  public Batch toBatch() {
    return new HybridSparkBatch(                                                     // Slice 3
        JavaSparkContext.fromSparkContext(spark.sparkContext()),
        table(),
        fileIO,
        readConf,
        groupingKeyType(),
        taskGroups(),
        expectedSchema(),
        hashCode(),
        readerConfig);
  }

  @Override
  protected synchronized List<? extends ScanTaskGroup<?>> taskGroups() {
    if (taskGroups == null) {
      this.taskGroups =
          planHomogeneousTaskGroups(
              fileTasks(),
              streamTasks(),
              fileScan != null ? adjustSplitSize(fileTasks(), fileScan.targetSplitSize()) : 0L,
              fileScan != null ? fileScan.splitLookback() : 1,
              fileScan != null ? fileScan.splitOpenFileCost() : 0L);
    }
    return taskGroups;
  }

  private synchronized List<FileScanTask> fileTasks() {
    if (fileTasks == null) {
      if (fileScan == null) {
        this.fileTasks = Lists.newArrayList();
      } else {
        try (CloseableIterable<? extends ScanTask> tasks = fileScan.planFiles()) {
          List<FileScanTask> planned = Lists.newArrayList();
          for (ScanTask task : tasks) {
            ValidationException.check(
                task instanceof FileScanTask,
                "Unsupported task type for file half of HybridSparkScan, expected FileScanTask: %s",
                task.getClass().getName());
            planned.add((FileScanTask) task);
          }
          this.fileTasks = planned;
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to close file scan: " + fileScan, e);
        }
      }
    }
    return fileTasks;
  }

  private synchronized List<BqStreamScanTask> streamTasks() {
    if (streamTasks == null) {
      this.streamTasks =
          Lists.newArrayList(bqPlanner.planStreams(table(), expectedSchema(), filterExpressions()));
    }
    return streamTasks;
  }

  /**
   * Bin-packs file and stream tasks into separate, homogeneous task groups and concatenates the
   * results. Exposed package-private so it can be tested without constructing a {@link SparkScan}.
   */
  static List<ScanTaskGroup<ScanTask>> planHomogeneousTaskGroups(
      List<? extends FileScanTask> fileTasks,
      List<? extends BqStreamScanTask> streamTasks,
      long splitSize,
      int splitLookback,
      long splitOpenFileCost) {
    List<ScanTaskGroup<ScanTask>> groups = Lists.newArrayList();

    if (!fileTasks.isEmpty()) {
      List<? extends ScanTaskGroup<? extends FileScanTask>> fileGroups =
          TableScanUtil.planTaskGroups(
              Lists.newArrayList(fileTasks), splitSize, splitLookback, splitOpenFileCost);
      for (ScanTaskGroup<? extends FileScanTask> group : fileGroups) {
        groups.add(upcastGroup(group));
      }
    }

    if (!streamTasks.isEmpty()) {
      List<? extends ScanTaskGroup<? extends BqStreamScanTask>> streamGroups =
          TableScanUtil.planTaskGroups(
              Lists.newArrayList(streamTasks), splitSize, splitLookback, splitOpenFileCost);
      for (ScanTaskGroup<? extends BqStreamScanTask> group : streamGroups) {
        groups.add(upcastGroup(group));
      }
    }
    return groups;
  }

  @SuppressWarnings("unchecked")
  private static ScanTaskGroup<ScanTask> upcastGroup(ScanTaskGroup<? extends ScanTask> group) {
    return (ScanTaskGroup<ScanTask>) group;
  }
}
```

### 6.5 Why the bin-packing strategy works

`TableScanUtil.planTaskGroups(List<T>, long, int, long)` takes any `T extends ScanTask` and:

- Splits each task via `SplittableScanTask.split(splitSize)` if it implements that interface
  (we deliberately make `BqStreamScanTask` *not* implement it — streams are pre-split by
  `GenerateScanPlan`).
- Bin-packs by `task.sizeBytes()` weight via `BinPacking.PackingIterable`.
- Merges adjacent tasks via `MergeableScanTask.canMerge`/`merge` if implemented (we don't
  implement it — homogeneous packing is enough).

Since we call `planTaskGroups` **separately** for file tasks and stream tasks, the two halves
can never end up in the same group. That's the entire homogeneity guarantee: it's an artifact
of running bin-packing on disjoint inputs, not a constraint we encode in `TableScanUtil`.

### 6.6 `SparkScanBuilder` modification

Just a TODO comment marking the dispatch point. The real `bq-advanced` mode detection is
deferred to Slice 4 (the test-and-mode-detection logic depends on Dataproc-side semantics that
aren't part of this slice).

```java
// in SparkScanBuilder.buildBatchScan(), wrapping the existing return statement
private Scan buildBatchScan() {
  Schema expectedSchema = schemaWithMetadataColumns();
  // TODO(bq-advanced): when the table is in bq-advanced mode, route to HybridSparkScan
  //   so that the BigQuery Storage Read API stream half is planned alongside the GCS file half.
  //   Detection of bq-advanced mode (table property, catalog signal, or BigLake metadata) and
  //   wiring of a real BqAdvancedScanPlanner that calls GenerateScanPlan are tracked in the
  //   HybridStream Slice 2 follow-up.
  return new SparkBatchQueryScan(
      spark,
      table,
      buildIcebergBatchScan(false /* not include Column Stats */, expectedSchema),
      readConf,
      expectedSchema,
      filterExpressions,
      metricsReporter::scanReport);
}
```

### 6.7 Tests — what to write and what *not* to write

Test the static helper `HybridSparkScan.planHomogeneousTaskGroups(...)` directly. **Do not**
test `HybridSparkScan` end-to-end in this slice — that requires a `SparkSession`, a real
Iceberg table, and is the right scope for Slice 4 (or an explicit integration test).

Five cases:

1. **Empty inputs → empty output.**
2. **Only file tasks → file-only groups.**
3. **Only stream tasks → stream-only groups.**
4. **Mixed → every group homogeneous; total task count preserved.**
5. **Stream tasks neither split nor merged** (large + small streams pass through unchanged).

#### Pitfall: `MockFileScanTask(long length)` crashes in `TableScanUtil.planTaskGroups`

[`MockFileScanTask`](../../core/src/test/java/org/apache/iceberg/MockFileScanTask.java) is the
obvious choice for fake `FileScanTask`s, but its `(long length)` constructor passes
`null` for the underlying `DataFile`:

```
java.lang.NullPointerException: Cannot invoke "ContentFile.format()" because "this.file" is null
  at BaseContentScanTask.split(BaseContentScanTask.java:101)
  at TableScanUtil.lambda$planTaskGroups$4(TableScanUtil.java:130)
```

`TableScanUtil` calls `task.split(splitSize)` because `FileScanTask` is a `SplittableScanTask`,
which dereferences the file. **Use Mockito to stub `FileScanTask` directly** instead, and
include a `split` stub that returns the mock itself:

```java
private static FileScanTask mockFileScanTask(long sizeBytes) {
  FileScanTask task = mock(FileScanTask.class);
  when(task.sizeBytes()).thenReturn(sizeBytes);
  when(task.length()).thenReturn(sizeBytes);
  when(task.filesCount()).thenReturn(1);
  when(task.estimatedRowsCount()).thenReturn(sizeBytes / 100);
  when(task.isFileScanTask()).thenReturn(true);
  when(task.asFileScanTask()).thenReturn(task);
  when(task.split(org.mockito.ArgumentMatchers.anyLong())).thenReturn(ImmutableList.of(task));
  return task;
}
```

For `BqStreamScanTask`, the real `BaseBqStreamScanTask` is fine — it's not splittable so
`TableScanUtil` doesn't try to dereference anything that's null.

### 6.8 Verification

```bash
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:compileJava
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:test \
    --tests "org.apache.iceberg.spark.source.TestHybridSparkScan"
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:checkstyleMain \
    :iceberg-spark:iceberg-spark-3.5_2.12:checkstyleTest \
    :iceberg-spark:iceberg-spark-3.5_2.12:spotlessJavaCheck
```

Iceberg's settings.gradle requires `-DsparkVersions=3.5` (or comma-separated list) to include
the Spark 3.5 module — without it, Gradle won't even know the project exists and you'll get
"project not found" errors.

---

## 7. Slice 3 — Executor-side router + BigQuery reader

### 7.1 Files

| Path | Action |
|---|---|
| `gradle/libs.versions.toml` | MODIFY: pin `spark-bigquery-connector = "0.42.4"` + library entries |
| `build.gradle` | MODIFY: exempt `iceberg-spark-3.5_*` from the global Guava exclusion |
| `spark/v3.5/build.gradle` | MODIFY: add `implementation` deps |
| `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/HybridReaderConfig.java` | NEW: serializable bundle of connector inputs |
| `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/BigQueryStreamColumnarReader.java` | NEW: `PartitionReader<ColumnarBatch>` over the connector |
| `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/HybridColumnarReaderFactory.java` | NEW: per-partition router |
| `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/HybridSparkBatch.java` | NEW: `SparkBatch` subclass wrapping factory in router |
| `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/HybridSparkScan.java` | MODIFY (from Slice 2): override `toBatch()` |
| `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/source/TestHybridColumnarReaderFactory.java` | NEW |
| `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/source/TestBigQueryStreamColumnarReader.java` | NEW |

### 7.2 The connector class we're wrapping — `ArrowInputPartitionContext`

The `spark-bigquery-connector` already has a battle-tested gRPC → Arrow → Spark `ColumnarBatch`
pipeline. The **public** entry point at the partition level is
`com.google.cloud.spark.bigquery.v2.context.ArrowInputPartitionContext`. Its constructor
signature (verbatim from connector source):

```java
public ArrowInputPartitionContext(
    BigQueryClientFactory bigQueryReadClientFactory,        // creds + ReadRowsClient cache
    BigQueryTracerFactory tracerFactory,                    // tracing
    List<String> names,                                      // stream names
    ReadRowsHelper.Options options,                          // retries, threads, endpoint
    ImmutableList<String> selectedFields,                    // projection
    ReadSessionResponse readSessionResponse,                 // accessed only for arrow schema
    Optional<StructType> userProvidedSchema,                 // optional Spark schema override
    SparkBigQueryReadSessionMetrics sparkBigQueryReadSessionMetrics,  // accumulators
    ResponseCompressionCodec responseCompressionCodec)
```

Calling `createPartitionReaderContext()` on the result returns an
`InputPartitionReaderContext<ColumnarBatch>`:

```java
public interface InputPartitionReaderContext<T> extends Closeable {
  boolean next() throws IOException;
  T get();
  Optional<BigQueryStorageReadRowsTracer> getBigQueryStorageReadRowsTracer();
}
```

Two facts about the lookup of fields on `readSessionResponse`:

- `ArrowInputPartitionContext` only reads
  `readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema()` from it. The
  `TableInfo` half can be `null`.
- `SparkBigQueryReadSessionMetrics` reads `readSession.getName()` from a separately-passed
  `ReadSession` at metrics-construction time (driver-side).

This means: **on the executor we synthesize a minimal `ReadSession` proto** containing only
the arrow schema bytes (which we have on the task) and the read session name (also on the task).

### 7.3 `HybridReaderConfig` — full source

A serializable bundle of the five connector inputs that need to be shipped from driver to
executors. All component types are already `Serializable` on the connector side
(`BigQueryClientFactory` serializes credentials as a byte array, etc.).

```java
// spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/HybridReaderConfig.java
package org.apache.iceberg.spark.source;

import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.cloud.spark.bigquery.metrics.SparkBigQueryReadSessionMetrics;
import java.io.Serializable;

/**
 * Executor-side configuration for {@link HybridColumnarReaderFactory}: the
 * spark-bigquery-connector factories needed to open a BigQuery Storage Read API gRPC stream.
 *
 * <p>Credential vending into {@link BigQueryClientFactory} is intentionally out of scope for
 * Slice 3; the {@link BigQueryClientFactory} passed in is expected to carry credentials by the
 * time the {@link HybridSparkScan} is built. Slice 4 will resolve the exact vending mechanism.
 */
public final class HybridReaderConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private final BigQueryClientFactory clientFactory;
  private final BigQueryTracerFactory tracerFactory;
  private final ReadRowsHelper.Options readOptions;
  private final ResponseCompressionCodec compressionCodec;
  private final SparkBigQueryReadSessionMetrics sessionMetrics;

  public HybridReaderConfig(
      BigQueryClientFactory clientFactory,
      BigQueryTracerFactory tracerFactory,
      ReadRowsHelper.Options readOptions,
      ResponseCompressionCodec compressionCodec,
      SparkBigQueryReadSessionMetrics sessionMetrics) {
    this.clientFactory = clientFactory;
    this.tracerFactory = tracerFactory;
    this.readOptions = readOptions;
    this.compressionCodec = compressionCodec;
    this.sessionMetrics = sessionMetrics;
  }

  public BigQueryClientFactory clientFactory()           { return clientFactory; }
  public BigQueryTracerFactory tracerFactory()           { return tracerFactory; }
  public ReadRowsHelper.Options readOptions()            { return readOptions; }
  public ResponseCompressionCodec compressionCodec()     { return compressionCodec; }
  public SparkBigQueryReadSessionMetrics sessionMetrics(){ return sessionMetrics; }
}
```

### 7.4 `BigQueryStreamColumnarReader` — full source

Two design choices to highlight:

- **Lazy delegate construction.** The connector's `ArrowInputPartitionContext.createPartitionReaderContext()`
  calls `TaskContext.get().registerAccumulator(...)` — that requires being inside a Spark task.
  Calling it from the constructor (driver-side or in tests) would NPE. Build the delegate on
  first `next()`.
- **Test-only constructor.** Takes a pre-built `InputPartitionReaderContext<ColumnarBatch>` so
  unit tests can mock the entire downstream pipeline.

```java
// spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/BigQueryStreamColumnarReader.java
package org.apache.iceberg.spark.source;

import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.v2.context.ArrowInputPartitionContext;
import com.google.cloud.spark.bigquery.v2.context.InputPartitionReaderContext;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Collection;
import org.apache.iceberg.BqStreamScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class BigQueryStreamColumnarReader implements PartitionReader<ColumnarBatch> {

  private final SparkInputPartition partition;
  private final HybridReaderConfig config;
  private InputPartitionReaderContext<ColumnarBatch> delegate;

  BigQueryStreamColumnarReader(SparkInputPartition partition, HybridReaderConfig config) {
    Preconditions.checkArgument(partition != null, "Invalid partition: null");
    Preconditions.checkArgument(config != null, "Invalid HybridReaderConfig: null");
    Preconditions.checkArgument(
        partition.allTasksOfType(BqStreamScanTask.class),
        "All tasks in the partition must be BqStreamScanTask: %s",
        partition);
    this.partition = partition;
    this.config = config;
  }

  /** Test-only constructor that injects an already-built context. */
  BigQueryStreamColumnarReader(InputPartitionReaderContext<ColumnarBatch> delegate) {
    this.partition = null;
    this.config = null;
    this.delegate = delegate;
  }

  @Override
  public boolean next() throws IOException { return delegate().next(); }

  @Override
  public ColumnarBatch get() { return delegate().get(); }

  @Override
  public void close() throws IOException {
    if (delegate != null) {
      delegate.close();
    }
  }

  private InputPartitionReaderContext<ColumnarBatch> delegate() {
    if (delegate == null) {
      this.delegate = openDelegate();
    }
    return delegate;
  }

  private InputPartitionReaderContext<ColumnarBatch> openDelegate() {
    Collection<? extends ScanTask> tasks = partition.<ScanTask>taskGroup().tasks();
    Preconditions.checkState(
        !tasks.isEmpty(), "Cannot open BigQueryStreamColumnarReader: no tasks");

    com.google.common.collect.ImmutableList.Builder<String> streamNames =
        com.google.common.collect.ImmutableList.builder();
    BqStreamScanTask first = null;
    for (ScanTask task : tasks) {
      BqStreamScanTask stream = task.asBqStreamScanTask();
      streamNames.add(stream.streamName());
      if (first == null) {
        first = stream;
      }
    }

    com.google.common.collect.ImmutableList<String> selectedFields =
        com.google.common.collect.ImmutableList.copyOf(first.selectedFields());
    ReadSessionResponse sessionResponse =
        new ReadSessionResponse(synthesizeReadSession(first), null);  // TableInfo unused

    ArrowInputPartitionContext arrowContext =
        new ArrowInputPartitionContext(
            config.clientFactory(),
            config.tracerFactory(),
            streamNames.build(),
            config.readOptions(),
            selectedFields,
            sessionResponse,
            java.util.Optional.empty(),
            config.sessionMetrics(),
            config.compressionCodec());
    return arrowContext.createPartitionReaderContext();
  }

  private static ReadSession synthesizeReadSession(BqStreamScanTask task) {
    return ReadSession.newBuilder()
        .setName(task.readSessionName())
        .setArrowSchema(
            ArrowSchema.newBuilder()
                .setSerializedSchema(ByteString.copyFrom(task.serializedArrowSchema()))
                .build())
        .build();
  }
}
```

#### Why fully-qualified `com.google.common.collect.ImmutableList`

The connector's `ArrowInputPartitionContext` constructor takes `com.google.common.collect.ImmutableList`
(unrelocated Guava), not Iceberg's relocated `org.apache.iceberg.relocated.com.google.common.collect.ImmutableList`.
Iceberg-spark code generally must avoid unrelocated Guava (the build excludes it globally). To keep the
import surface clean, we use fully-qualified references *only* at the connector boundary
(this single file). Everywhere else in the spark-3.5 module we still use the relocated variant.

#### `ScanTaskGroup.tasks()` returns `Collection`, not `List`

Don't try to type the local as `List<? extends ScanTask>`. The api method is
`Collection<T> tasks();` — assign to `Collection<? extends ScanTask>` and iterate.

### 7.5 `HybridColumnarReaderFactory` — full source

The router. Two facts that make this clean:

- `SparkInputPartition.allTasksOfType(Class<T>)` is already a public method; we don't need to
  add anything there.
- Bin-packing in Slice 2 already guarantees task-type homogeneity per partition, so a single
  `allTasksOfType(BqStreamScanTask.class)` check suffices.

```java
// spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/HybridColumnarReaderFactory.java
package org.apache.iceberg.spark.source;

import org.apache.iceberg.BqStreamScanTask;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class HybridColumnarReaderFactory implements PartitionReaderFactory {

  private static final long serialVersionUID = 1L;

  private final PartitionReaderFactory fileDelegate;
  private final HybridReaderConfig readerConfig;

  HybridColumnarReaderFactory(
      PartitionReaderFactory fileDelegate, HybridReaderConfig readerConfig) {
    Preconditions.checkArgument(fileDelegate != null, "Invalid file delegate factory: null");
    Preconditions.checkArgument(readerConfig != null, "Invalid HybridReaderConfig: null");
    this.fileDelegate = fileDelegate;
    this.readerConfig = readerConfig;
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    if (isStreamPartition(partition)) {
      return true;                                                // BQ streams are always Arrow
    }
    return fileDelegate.supportColumnarReads(partition);
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    if (isStreamPartition(partition)) {
      return new BigQueryStreamColumnarReader((SparkInputPartition) partition, readerConfig);
    }
    return fileDelegate.createColumnarReader(partition);
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    Preconditions.checkArgument(
        !isStreamPartition(partition),
        "BqStreamScanTask partitions only support columnar reads: %s",
        partition);
    return fileDelegate.createReader(partition);
  }

  private static boolean isStreamPartition(InputPartition partition) {
    return partition instanceof SparkInputPartition
        && ((SparkInputPartition) partition).allTasksOfType(BqStreamScanTask.class);
  }
}
```

### 7.6 `HybridSparkBatch` — full source

`SparkBatch.createReaderFactory()` would normally pick `SparkColumnarReaderFactory(parquetConf)`,
`SparkColumnarReaderFactory(orcConf)`, or `SparkRowReaderFactory()` based on whether *all*
task groups are vectorizable. With a heterogeneous task list it falls through to the row reader
— the wrong fallback for the file half. Override.

**Slice 3 simplification:** the file-half delegate is hard-coded to Parquet vectorized. This
is correct for the BigLake/Managed-Iceberg primary case (BigQuery writes Parquet). Slice 4
will refine by inspecting only file groups for ORC and delete-laden cases. Two design notes:

- `SparkBatch`'s task group field and `useParquetBatchReads`/`useOrcBatchReads` predicates are
  `private`, so we cannot directly filter them from a subclass. The cleanest upstream-friendly
  fix in Slice 4 is to make those `protected` (a small SparkBatch change) and override; for
  Slice 3 the duplicate `readConf` field plus a hard-coded Parquet path is sufficient.
- The constructor exactly mirrors `SparkBatch`'s, with one extra parameter for `HybridReaderConfig`.

```java
// spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/HybridSparkBatch.java
package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.ImmutableParquetBatchReadConf;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

class HybridSparkBatch extends SparkBatch {

  private final SparkReadConf readConf;
  private final HybridReaderConfig readerConfig;

  HybridSparkBatch(
      JavaSparkContext sparkContext,
      Table table,
      Supplier<FileIO> fileIO,
      SparkReadConf readConf,
      Types.StructType groupingKeyType,
      List<? extends ScanTaskGroup<?>> taskGroups,
      Schema expectedSchema,
      int scanHashCode,
      HybridReaderConfig readerConfig) {
    super(
        sparkContext, table, fileIO, readConf, groupingKeyType, taskGroups,
        expectedSchema, scanHashCode);
    this.readConf = readConf;
    this.readerConfig = readerConfig;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    PartitionReaderFactory fileDelegate =
        new SparkColumnarReaderFactory(
            ImmutableParquetBatchReadConf.builder().batchSize(readConf.parquetBatchSize()).build());
    return new HybridColumnarReaderFactory(fileDelegate, readerConfig);
  }
}
```

### 7.7 `HybridSparkScan.toBatch()` — diff vs Slice 2

The Slice 2 `HybridSparkScan` had no `toBatch()` override (it inherited `SparkScan`'s default,
which would build a vanilla `SparkBatch` and then fall through to the row reader). Slice 3
adds the override and the `readerConfig` field. The full file is in Section 6.4 above.

Key parts of the diff:

1. New imports: `org.apache.iceberg.io.FileIO`, `org.apache.spark.api.java.JavaSparkContext`,
   `org.apache.spark.sql.connector.read.Batch`.
2. New private fields: `spark`, `fileIO`, `readConf`, `readerConfig`.
3. New constructor parameter: `HybridReaderConfig readerConfig`. Stored in the new field.
4. New `@Override toBatch()` returning `new HybridSparkBatch(...)` (see Section 6.4).

### 7.8 Tests — what to write

**Two test classes, both Mockito-only.** Don't try to actually run a gRPC stream — that's
Slice 4. Just verify the routing logic and the delegation pattern.

#### `TestHybridColumnarReaderFactory`

Seven cases:

1. **`streamPartitionRoutesToBigQueryReader`** — `supportColumnarReads(streamPartition)` true,
   delegate's `supportColumnarReads` never called.
2. **`filePartitionDelegatesSupportColumnarReads`** — delegate's method *is* called for file
   partitions.
3. **`filePartitionDelegatesColumnarReader`** — `createColumnarReader(filePartition)` returns
   exactly what the delegate returns.
4. **`filePartitionDelegatesRowReader`** — same, for `createReader`.
5. **`streamPartitionRowReaderThrows`** — `createReader(streamPartition)` throws
   `IllegalArgumentException` with a clear message.
6. **`rejectsNullDelegate`** / **`rejectsNullReaderConfig`** — `Preconditions` fire.

Use `Mockito.mock(SparkInputPartition.class)` and stub
`when(partition.allTasksOfType(BqStreamScanTask.class)).thenReturn(true|false)` to fake
each side of the router.

#### `TestBigQueryStreamColumnarReader`

Four cases:

1. **`delegatesNextGetClose`** — using the test-only constructor with a mocked
   `InputPartitionReaderContext`, verify `next`/`get`/`close` round-trip correctly.
2. **`closeIsNoOpWhenContextNeverOpened`** — close before next() is called must not invoke
   `partition.taskGroup()` or any other openDelegate logic. (Verifies the lazy guard.)
3. **`rejectsNonStreamPartition`** — production constructor throws if
   `allTasksOfType(BqStreamScanTask.class)` is false.
4. **`rejectsNullArguments`** — both constructor preconditions fire.

### 7.9 Verification

```bash
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:classes
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:test \
    --tests "org.apache.iceberg.spark.source.TestHybridColumnarReaderFactory" \
    --tests "org.apache.iceberg.spark.source.TestBigQueryStreamColumnarReader" \
    --tests "org.apache.iceberg.spark.source.TestHybridSparkScan"
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:checkstyleMain \
    :iceberg-spark:iceberg-spark-3.5_2.12:checkstyleTest \
    :iceberg-spark:iceberg-spark-3.5_2.12:spotlessJavaCheck
```

---

## 8. Build configuration changes

These three edits are all that's needed to get the connector deps onto the spark-3.5 classpath
without breaking other modules.

### 8.1 `gradle/libs.versions.toml`

Two additions: a version pin and two library coordinates. Keep them in alphabetical order
within their respective sections.

```toml
# In the [versions] block (alphabetical order, between bouncycastle/bson-ver and caffeine):
# spark-bigquery-connector libraries used by the HybridStream BigQuery integration in
# iceberg-spark-3.5. Bumping requires re-checking the public constructor of
# com.google.cloud.spark.bigquery.v2.context.ArrowInputPartitionContext (Slice 1 task model).
spark-bigquery-connector = "0.42.4"
```

```toml
# In the [libraries] block (after the snowflake-jdbc entry, before the test libraries section):
spark-bigquery-connector-common = { module = "com.google.cloud.spark:bigquery-connector-common", version.ref = "spark-bigquery-connector" }
spark-bigquery-dsv2-common = { module = "com.google.cloud.spark:spark-bigquery-dsv2-common", version.ref = "spark-bigquery-connector" }
```

### 8.2 `spark/v3.5/build.gradle`

Add two `implementation` blocks alongside the existing `implementation libs.caffeine` line.
Excludes mirror the existing iceberg-arrow conventions to avoid double-pulling things Iceberg
already manages.

```gradle
// HybridStream BigQuery integration: lean libs from spark-bigquery-connector for the
// executor-side Arrow -> ColumnarBatch path. We deliberately avoid the umbrella
// spark-3.5-bigquery_2.12 jar so the connector's own DSv2 TableProvider does not
// collide with Iceberg's. Excludes mirror the existing iceberg-arrow conventions.
implementation(libs.spark.bigquery.dsv2.common) {
  exclude group: 'org.apache.spark'
  exclude group: 'org.apache.arrow'
  exclude group: 'io.netty'
  exclude group: 'org.scala-lang'
}
implementation(libs.spark.bigquery.connector.common) {
  exclude group: 'org.apache.arrow'
  exclude group: 'io.netty'
}
```

### 8.3 Root `build.gradle` Guava exclusion exemption

The root `subprojects` block excludes unshaded Guava from the `compileClasspath` of every
module except `iceberg-bundled-guava`:

```gradle
// before
if (project.name != 'iceberg-bundled-guava') {
  exclude group: 'com.google.guava', module: 'guava'
}
```

The connector's `ArrowInputPartitionContext` constructor takes
`com.google.common.collect.ImmutableList<String>` (the unshaded variant). With the global
exclusion in force, that import doesn't resolve. Exempt the Spark 3.5 and 4.0 modules:

```gradle
// after
// iceberg-spark-3.5_* and iceberg-spark-4.0_* also need unshaded Guava on their
// classpath because the spark-bigquery-connector's ArrowInputPartitionContext API
// takes a com.google.common.collect.ImmutableList (HybridStream BigQuery integration).
if (project.name != 'iceberg-bundled-guava'
    && !project.name.startsWith('iceberg-spark-3.5_')
    && !project.name.startsWith('iceberg-spark-4.0_')) {
  exclude group: 'com.google.guava', module: 'guava'
}
```

This is the smallest possible blast-radius change; all other modules still see only
relocated Guava. When porting to a new Spark version (see [§9](#9-porting-to-additional-spark-versions)),
add another `&& !project.name.startsWith('iceberg-spark-X.Y_')` clause.

### 8.4 Sanity-check the dependency graph

After the edits:

```bash
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:dependencies \
    --configuration runtimeClasspath | grep -E "spark-bigquery|bigquery-connector"
```

Expected output (truncated):

```
+--- com.google.cloud.spark:spark-bigquery-dsv2-common:0.42.4
|    +--- com.google.cloud.spark:spark-bigquery-connector-common:0.42.4
|    |    +--- com.google.cloud.spark:bigquery-connector-common:0.42.4
\--- com.google.cloud.spark:bigquery-connector-common:0.42.4 (*)
```

---

## 9. Porting to additional Spark versions

Slice 1 lives in `iceberg-api` + `iceberg-core` and is shared across all Spark versions
automatically — no per-version code there.

Slices 2 + 3 live in `spark/v3.5/spark/.../source/`. Iceberg keeps parallel module trees
per Spark version (`spark/v3.4/`, `spark/v3.5/`, `spark/v4.0/`, `spark/v4.1/`, …) with
roughly identical source. **For HybridStream specifically, the underlying classes we touch
or extend (`SparkScan`, `SparkBatch`, `SparkInputPartition`, `SparkColumnarReaderFactory`,
`ParquetBatchReadConf`) are byte-identical between Spark 3.5 and 4.0 in this Iceberg base** —
so the v4.0 port was a verbatim file copy with no source-level changes. The same is likely
to hold for any future Spark 3.x → 4.x port; verify with `diff -q` first.

### 9.1 Recipe

For a target Spark version `X.Y` with Scala suffix `_S.S` (e.g. `4.0` + `_2.13`):

1. **Confirm the underlying classes are unchanged** between `spark/v3.5/spark/...` and
   `spark/vX.Y/spark/...`:
   ```bash
   for f in SparkScan SparkBatch SparkInputPartition SparkColumnarReaderFactory; do
     diff -q spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/$f.java \
             spark/vX.Y/spark/src/main/java/org/apache/iceberg/spark/source/$f.java
   done
   diff -q spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/ParquetBatchReadConf.java \
           spark/vX.Y/spark/src/main/java/org/apache/iceberg/spark/ParquetBatchReadConf.java
   ```
   If anything differs, read the diff before proceeding — the new Spark version may have
   reshaped a base class and the HybridStream code may need a tweak.

2. **Copy the six new main files + three test files verbatim**:
   ```bash
   SRC_M=spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source
   DST_M=spark/vX.Y/spark/src/main/java/org/apache/iceberg/spark/source
   SRC_T=spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/source
   DST_T=spark/vX.Y/spark/src/test/java/org/apache/iceberg/spark/source
   for f in BqAdvancedScanPlanner HybridReaderConfig BigQueryStreamColumnarReader \
            HybridColumnarReaderFactory HybridSparkBatch HybridSparkScan; do
     cp "$SRC_M/$f.java" "$DST_M/$f.java"
   done
   for f in TestHybridSparkScan TestHybridColumnarReaderFactory TestBigQueryStreamColumnarReader; do
     cp "$SRC_T/$f.java" "$DST_T/$f.java"
   done
   ```

3. **Replicate the `TODO(bq-advanced)` comment** at the top of
   `spark/vX.Y/spark/.../SparkScanBuilder.java`'s `buildBatchScan()` method
   (see [§6.6](#66-sparkscanbuilder-modification) for the verbatim text).

4. **Add the connector deps** to `spark/vX.Y/build.gradle`. Anchor: the line
   `implementation libs.caffeine`. Insert immediately after, with the same block of excludes
   shown in [§8.2](#82-sparkv35buildgradle).

5. **Widen the root `build.gradle` Guava-exclusion exemption** to include `iceberg-spark-X.Y_*`
   (see [§8.3](#83-root-buildgradle-guava-exclusion-exemption)).

6. **Verify** with `-DsparkVersions=X.Y`:
   ```bash
   ./gradlew -DsparkVersions=X.Y :iceberg-spark:iceberg-spark-X.Y_S.S:classes
   ./gradlew -DsparkVersions=X.Y :iceberg-spark:iceberg-spark-X.Y_S.S:test \
       --tests "*Hybrid*" --tests "*BigQueryStream*"
   ./gradlew -DsparkVersions=X.Y :iceberg-spark:iceberg-spark-X.Y_S.S:checkstyleMain \
       :iceberg-spark:iceberg-spark-X.Y_S.S:checkstyleTest \
       :iceberg-spark:iceberg-spark-X.Y_S.S:spotlessJavaCheck
   ```
   Should produce 16 passing tests across the three test classes (5 + 7 + 4) — same
   count as v3.5 and v4.0.

### 9.2 Worth noting on Scala 2.12 vs 2.13

Spark 4.0 uses Scala 2.13, Spark 3.5 uses Scala 2.12. The HybridStream source code uses
no Scala APIs, so the per-Scala compile is incidental. The connector libraries
(`bigquery-connector-common`, `spark-bigquery-dsv2-common`) are not Scala-suffixed (they're
plain Java) and the same artifact resolves cleanly for both Scala versions.

### 9.3 When the copy approach starts hurting

For Slice 4 work and beyond, six new classes per Spark version means each bug-fix or
refactor doubles. If you expect HybridStream to keep growing (more readers, schema mapping
helpers, splittable-stream logic), consider extracting an
`iceberg-spark-hybridstream-common` module that both `iceberg-spark-3.5_*` and
`iceberg-spark-4.0_*` depend on. Build it with `compileOnly` against a Spark API surface
that's stable across versions (`SparkScan`, `SparkBatch`, `SparkInputPartition`, the
DSv2 reader factories) so it doesn't itself become Spark-version-specific. As of writing,
the duplication is small enough that copy-paste is still the right call.

---

## 10. Verification commands

A consolidated verification recipe per slice. Each slice's tests should be a strict superset of
the previous slice's still passing.

### After Slice 1

```bash
./gradlew :iceberg-api:compileJava :iceberg-core:compileJava
./gradlew :iceberg-api:test --tests TestScanTaskDiscrimination
./gradlew :iceberg-core:test --tests TestBaseBqStreamScanTask
./gradlew :iceberg-api:checkstyleMain :iceberg-api:checkstyleTest \
          :iceberg-core:checkstyleMain :iceberg-core:checkstyleTest \
          :iceberg-api:spotlessJavaCheck :iceberg-core:spotlessJavaCheck
```

### After Slice 2

```bash
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:compileJava
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:test \
    --tests "org.apache.iceberg.spark.source.TestHybridSparkScan"
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:checkstyleMain \
    :iceberg-spark:iceberg-spark-3.5_2.12:checkstyleTest \
    :iceberg-spark:iceberg-spark-3.5_2.12:spotlessJavaCheck
```

### After Slice 3

```bash
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:classes
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:test \
    --tests "org.apache.iceberg.spark.source.TestHybridColumnarReaderFactory" \
    --tests "org.apache.iceberg.spark.source.TestBigQueryStreamColumnarReader" \
    --tests "org.apache.iceberg.spark.source.TestHybridSparkScan"
./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:checkstyleMain \
    :iceberg-spark:iceberg-spark-3.5_2.12:checkstyleTest \
    :iceberg-spark:iceberg-spark-3.5_2.12:spotlessJavaCheck
```

If `spotlessJavaCheck` fails, run `./gradlew ...:spotlessApply` to auto-format.

### After the Spark 4.0 port

Run the same Slice 3 commands against the v4.0 module:

```bash
./gradlew -DsparkVersions=4.0 :iceberg-spark:iceberg-spark-4.0_2.13:classes
./gradlew -DsparkVersions=4.0 :iceberg-spark:iceberg-spark-4.0_2.13:test \
    --tests "org.apache.iceberg.spark.source.TestHybridColumnarReaderFactory" \
    --tests "org.apache.iceberg.spark.source.TestBigQueryStreamColumnarReader" \
    --tests "org.apache.iceberg.spark.source.TestHybridSparkScan"
./gradlew -DsparkVersions=4.0 :iceberg-spark:iceberg-spark-4.0_2.13:checkstyleMain \
    :iceberg-spark:iceberg-spark-4.0_2.13:checkstyleTest \
    :iceberg-spark:iceberg-spark-4.0_2.13:spotlessJavaCheck
```

Same 16-test pass count (5 + 7 + 4) as v3.5. Iceberg's `settings.gradle` only includes a
Spark version's modules when listed in `-DsparkVersions=...`, so without the flag Gradle
won't even know the project exists. To verify both at once: `-DsparkVersions=3.5,4.0`.

---

## 11. Pitfalls encountered (and how to avoid them)

### Slice 1

- **Kryo can't deserialize `ImmutableList`.** Store collection fields as `String[]` (or other
  primitive arrays) and lazy-wrap as `ImmutableList` via a `transient volatile` cache. Same
  pattern as `BaseFileScanTask.deletes`.
- **Iceberg's checkstyle requires `assertThatThrownBy(...).hasMessage*(...)`.** A bare
  `isInstanceOf` will fail with `"assertThatThrownBy must include a message check"`.

### Slice 2

- **`MockFileScanTask(long)` NPEs in bin-packing.** Its `(long length)` constructor passes
  `null` for the data file; `TableScanUtil.planTaskGroups` then calls
  `BaseContentScanTask.split` which dereferences `file.format()`. Use Mockito to stub
  `FileScanTask` directly and stub `split(long)` to return the mock itself.
- **`Scan.planTasks()` already exists.** Don't add a method by that name. Don't try to make
  the api `Scan` heterogeneous; keep heterogeneity in the Spark adapter layer.
- **Spark version selector.** Iceberg's `settings.gradle` only includes a Spark version's
  modules when listed in `-DsparkVersions=...`. Without `-DsparkVersions=3.5` you'll see
  `project not found`.

### Slice 3

- **Connector requires unshaded Guava.** `ArrowInputPartitionContext`'s constructor takes
  `com.google.common.collect.ImmutableList`. Iceberg's global exclusion of unshaded Guava
  must be lifted **only** for the spark-3.5 module. Use fully-qualified
  `com.google.common.collect.ImmutableList` references at the connector boundary so the rest
  of the file/import block stays clean.
- **`ScanTaskGroup.tasks()` returns `Collection`, not `List`.** Type the iterator variable
  accordingly.
- **`SparkBigQueryReadSessionMetrics` has a private constructor.** Use the static factory
  `from(sparkSession, readSession, ...)`. This means the metrics object is constructed on
  the **driver** (where `SparkSession` is available) and passed via `HybridReaderConfig`.
- **`TaskContext.get()` is null on the driver.** The connector's
  `createPartitionReaderContext()` registers Spark accumulators against `TaskContext.get()`.
  Always construct the `ArrowInputPartitionContext` lazily, on the *first* `next()` call.
- **Don't subclass `SparkBatchQueryScan` for the Hybrid path.** It extends
  `SparkPartitioningAwareScan`, which assumes every task implements `PartitionScanTask`.
  `BqStreamScanTask` doesn't. Subclass `SparkScan` directly.
- **`SparkBatch`'s `taskGroups`/`readConf` are private.** Re-declaring `readConf` as a
  duplicate field in `HybridSparkBatch` is intentional. Slice 4 may want to widen
  `SparkBatch`'s visibility for cleaner overrides.

---

## 12. Known limitations and Slice 4 roadmap

These items are explicitly out of scope for Slices 1–3 and need work before HybridStream is
production-ready. Items 12.1–12.5 are the ones blocked on Google-internal knowledge
(google3, internal artifact registry, internal docs) — those are tracked formally in
[`slice-4-research-prompt.md`](slice-4-research-prompt.md), a structured discovery brief
that an internal Google research agent can execute. Read that file in tandem with this
section once Slice 4 starts.

### 12.1 `bq-advanced` table mode detection

`SparkScanBuilder.buildBatchScan()` has a TODO comment but no actual dispatch. Decide:

- Where does the `bq-advanced` signal come from? Table property, BigLake metadata API, catalog
  hint, Spark conf?
- Same scan or different builder for `bq-advanced` tables? (Recommendation: same builder, dispatch
  inside `buildBatchScan()`.)

### 12.2 Real `BqAdvancedScanPlanner` implementation

The SPI exists; nothing implements it. Wire a real planner that calls
`com.google.cloud.bigquery.storage.v1beta2.BigQueryRead.GenerateScanPlan` and constructs
`BaseBqStreamScanTask`s from the response. This needs:

- A BigQuery client (with credentials) on the driver.
- The connector's `ReadSessionCreator`/`BigQueryClientFactory` is reusable; consider whether
  to depend on it directly or call the proto stubs.

### 12.3 Credential vending

`HybridReaderConfig.clientFactory` must arrive at the executor with credentials baked in.
Two options:

1. **Single supplier:** the driver constructs one `BigQueryClientFactory` with broadcast-able
   credentials; same factory used for every partition. Simplest.
2. **Per-stream tokens from `GenerateScanPlan`:** if the API returns scoped tokens per stream,
   include them on `BqStreamScanTask` and synthesize a per-task `BigQueryClientFactory` on the
   executor. More secure for FGAC but more complex.

The user (Dataproc team) has flagged this as a Slice 4 decision.

### 12.4 File-half routing refinement in `HybridSparkBatch`

Today `HybridSparkBatch.createReaderFactory()` hard-codes Parquet vectorization. ORC tables and
Parquet tables with equality deletes will silently get the wrong reader. Fix: filter the task
groups to file-only, then run the same `useParquetBatchReads`/`useOrcBatchReads` predicates as
`SparkBatch` does. Cleanest path is making those predicates `protected` in `SparkBatch`.

### 12.5 End-to-end test with a fake/live BQ Storage server

Slice 3 unit tests use Mockito throughout. There is no test that actually opens a gRPC stream.
For Slice 4 add either:

- An integration test against a live BQ project (gated by env var with credentials).
- A fake gRPC server that serves canned `ReadRowsResponse`s, exercising the
  `BigQueryStreamColumnarReader → ArrowInputPartitionContext` path.

### 12.6 Schema / field-ID mapping

`BqStreamScanTask` carries the BQ Arrow schema as serialized bytes. The `ColumnarBatch` produced
by the connector uses BQ-derived column names; Iceberg downstream expects column resolution by
field ID. Slice 4 needs an `ArrowSchemaConverter` extension (or similar) that maps BQ Arrow
columns to Iceberg field IDs for correct projection / nested struct handling.

### 12.7 NQE (Native Query Engine) support

This whole design fixes Spark's DSv2 path. Dataproc's C++/Velox NQE is a separate execution
engine that won't traverse `HybridColumnarReaderFactory`. NQE integration is an entirely
separate effort.

### 12.8 Iceberg 1.11 REST `PlanTable` alignment

When Iceberg 1.11 lands, the planning logic moves to the REST catalog server side
(`PlanTable` API). The `BqAdvancedScanPlanner` SPI call should migrate from the
`SparkScanBuilder` to the REST catalog server. The task data model (`BqStreamScanTask`)
shouldn't need to change.

### 12.9 Stream skew

`BqStreamScanTask` is non-splittable and non-mergeable. If `GenerateScanPlan` returns very
uneven stream sizes, Spark task skew is possible. Options for Slice 5+: implement
`SplittableScanTask` (delegates to BQ Storage API server-side splitting if the API supports it)
or `MergeableScanTask`.

---

## Appendix A — External types we depend on

All from `spark-bigquery-connector` 0.42.4. Group is `com.google.cloud.spark` unless noted.

| Type | Artifact | Purpose |
|---|---|---|
| `com.google.cloud.spark.bigquery.v2.context.ArrowInputPartitionContext` | `spark-bigquery-dsv2-common` | Builds the executor-side reader context. **Public class.** |
| `com.google.cloud.spark.bigquery.v2.context.InputPartitionReaderContext<T>` | `spark-bigquery-dsv2-common` | Reader interface (`next`/`get`/`close`). |
| `com.google.cloud.spark.bigquery.metrics.SparkBigQueryReadSessionMetrics` | `spark-bigquery-connector-common` (transitive) | Spark accumulators. Driver-built via `from(SparkSession, ReadSession, ...)`. |
| `com.google.cloud.bigquery.connector.common.BigQueryClientFactory` | `bigquery-connector-common` | Creds + ReadRowsClient cache. Serializable. |
| `com.google.cloud.bigquery.connector.common.BigQueryTracerFactory` | `bigquery-connector-common` | Tracing. Serializable. |
| `com.google.cloud.bigquery.connector.common.ReadRowsHelper.Options` | `bigquery-connector-common` | `(maxRetries, Optional<endpoint>, backgroundThreads, prebuffer)`. |
| `com.google.cloud.bigquery.connector.common.ReadSessionResponse` | `bigquery-connector-common` | Plain `(ReadSession, TableInfo)` value class. `TableInfo` may be `null`. |
| `com.google.cloud.bigquery.storage.v1.ReadSession` | `google-cloud-bigquerystorage` 3.22.1+ | Proto. We synthesize a minimal one with name + ArrowSchema. |
| `com.google.cloud.bigquery.storage.v1.ArrowSchema` | (same) | Proto, serialized schema bytes inside. |
| `com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec` | (same) | Proto enum, snake-case packaging. |
| `com.google.protobuf.ByteString` | `protobuf-java` (transitive) | For wrapping the task's `byte[]` schema. |

---

## Appendix B — Iceberg internals referenced

Stable-ish surface; safe to depend on at the api / Spark adapter layer.

| Type | Path | Why |
|---|---|---|
| `org.apache.iceberg.ScanTask` | `api/src/main/java/org/apache/iceberg/ScanTask.java` | Marker interface; we add `is/asBqStreamScanTask` defaults. |
| `org.apache.iceberg.FileScanTask` | `api/src/main/java/org/apache/iceberg/FileScanTask.java` | Existing task type; pattern to mirror. |
| `org.apache.iceberg.ChangelogScanTask` | `api/src/main/java/org/apache/iceberg/ChangelogScanTask.java` | Closest interface-shape precedent for a new task type. |
| `org.apache.iceberg.BaseFileScanTask` | `core/src/main/java/org/apache/iceberg/BaseFileScanTask.java` | Concrete-impl precedent; transient/lazy-cache idiom for non-essential derived fields. |
| `org.apache.iceberg.util.TableScanUtil#planTaskGroups(List<T>, long, int, long)` | `core/src/main/java/org/apache/iceberg/util/TableScanUtil.java` | Bin-packing helper — generic on any `T extends ScanTask`. |
| `org.apache.iceberg.spark.source.SparkScan` | `spark/v3.5/.../SparkScan.java` | Abstract `Scan` impl; we extend directly (skip `SparkPartitioningAwareScan`). |
| `org.apache.iceberg.spark.source.SparkBatch` | `spark/v3.5/.../SparkBatch.java` | We subclass it for `HybridSparkBatch`. |
| `org.apache.iceberg.spark.source.SparkInputPartition` | `spark/v3.5/.../SparkInputPartition.java` | `allTasksOfType(Class)` helper is reused by the router. |
| `org.apache.iceberg.spark.source.SparkColumnarReaderFactory` | `spark/v3.5/.../SparkColumnarReaderFactory.java` | Used as the file-half delegate inside `HybridColumnarReaderFactory`. |
| `org.apache.iceberg.spark.source.SparkRowReaderFactory` | `spark/v3.5/.../SparkRowReaderFactory.java` | Prior art for "different task types → different reader implementations" within one factory. |
| `org.apache.iceberg.spark.SparkReadConf` | `spark/v3.5/.../spark/SparkReadConf.java` | `parquetBatchSize()` etc. |
| `org.apache.iceberg.api.TestHelpers` | `api/src/test/java/org/apache/iceberg/TestHelpers.java` | Java + Kryo round-trip test helpers (`KryoHelpers`, `serializers()` parameter source). |

---

*End of document.*
