# Slice 4 Research Brief — HybridStream BigQuery Integration

> **Audience:** an internal Google research/coding agent with access to google3, internal
> artifact registry, internal documentation (go/ links, BUILD files, internal proto repos),
> and the public Iceberg fork at <https://github.com/BoqianShi/iceberg>.
>
> **Output:** a single Markdown report (3–5 pages) answering five investigations.
> No code changes, no PRs.

---

## 1. Mission

Five questions need concrete, citation-backed answers before Slice 4 of the HybridStream
BigQuery integration in the Dataproc Iceberg fork can begin. Each is blocked on
Google-internal knowledge that the public-facing agent could not reach.

Produce a single report (Markdown) that:

1. Answers each of the five investigations below with citations to specific files (path +
   line number), proto messages, go/-links, or internal docs.
2. Calls out ambiguities, trade-offs, and decisions the human user must make before
   implementation can begin.
3. Ends with a recommended Slice 4 task order.

**Do not modify code. Do not open PRs. Do not run gradle/mvn builds.** Output is a written
report only.

---

## 2. Background — what's already been built

Slices 1–3 of HybridStream have shipped on the fork. Read
`dev-docs/hybrid-stream/implementation.md` on the fork (branch
`bq-stream-scan-task-design-doc` or any later branch) for the full file-level guide. Here
is the minimum context Slice 4 needs:

### 2.1 The data model (Slice 1, lives in iceberg-api / iceberg-core)

`org.apache.iceberg.BqStreamScanTask` — a `ScanTask` sibling to `FileScanTask`:

```java
public interface BqStreamScanTask extends ScanTask {
  String streamName();             // e.g. projects/.../sessions/.../streams/0
  String readSessionName();
  byte[] serializedArrowSchema();
  List<String> selectedFields();
  // estimatedRowsCount() and sizeBytes() inherited
}
```

Concrete impl: `BaseBqStreamScanTask`. Java + Kryo serializable (`String[]` storage trick
for `selectedFields`).

### 2.2 Driver-side planning SPI (Slice 2, in iceberg-spark-3.5_2.12 + iceberg-spark-4.0_2.13)

`org.apache.iceberg.spark.source.BqAdvancedScanPlanner`:

```java
public interface BqAdvancedScanPlanner {
  List<BqStreamScanTask> planStreams(
      Table table, Schema projection, List<Expression> filters);

  static BqAdvancedScanPlanner noop() { ... }   // returns empty list
}
```

Currently no real implementation exists. Slice 4 must wire one that calls
`GenerateScanPlan`.

`HybridSparkScan` extends `SparkScan`; runs `fileScan.planFiles()` for the GCS half and
`bqPlanner.planStreams(...)` for the stream half, then bin-packs them separately with
`TableScanUtil.planTaskGroups` to keep each `ScanTaskGroup` homogeneous.

### 2.3 Driver dispatch (TODO comment, Slice 2)

`SparkScanBuilder.buildBatchScan()` in both `spark/v3.5/.../SparkScanBuilder.java` and
`spark/v4.0/.../SparkScanBuilder.java` has:

```java
// TODO(bq-advanced): when the table is in bq-advanced mode, route to HybridSparkScan
//   so that the BigQuery Storage Read API stream half is planned alongside the GCS file half.
//   Detection of bq-advanced mode (table property, catalog signal, or BigLake metadata) and
//   wiring of a real BqAdvancedScanPlanner that calls GenerateScanPlan are tracked in the
//   HybridStream Slice 2 follow-up.
```

This is the dispatch site Slice 4 must replace with a real predicate + factory.

### 2.4 Executor-side router + reader (Slice 3)

`HybridColumnarReaderFactory` routes per-partition by `SparkInputPartition.allTasksOfType(...)`.
`BigQueryStreamColumnarReader` lazily constructs the spark-bigquery-connector's
`com.google.cloud.spark.bigquery.v2.context.ArrowInputPartitionContext` and delegates
`next/get/close` to the resulting `InputPartitionReaderContext<ColumnarBatch>`.

Executor-side connector inputs are bundled in `HybridReaderConfig`:

```java
public final class HybridReaderConfig implements Serializable {
  private final BigQueryClientFactory clientFactory;          // creds + ReadRowsClient cache
  private final BigQueryTracerFactory tracerFactory;
  private final ReadRowsHelper.Options readOptions;
  private final ResponseCompressionCodec compressionCodec;
  private final SparkBigQueryReadSessionMetrics sessionMetrics;
  // ...
}
```

Today the user is expected to construct and pass one in. There is no integration with
Dataproc's auth story yet.

### 2.5 What's pinned today

- Iceberg base: 1.10 line.
- Spark targets: 3.5 (Scala 2.12) and 4.0 (Scala 2.13) — both modules have the HybridStream
  classes verbatim.
- spark-bigquery-connector: `com.google.cloud.spark:spark-bigquery-dsv2-common:0.42.4`
  + `com.google.cloud.spark:bigquery-connector-common:0.42.4`. Pulled in via
  `gradle/libs.versions.toml`.
- `google-cloud-bigquerystorage:3.22.1` arrives transitively from the connector libs.
- Root `build.gradle` has a global Guava exclusion lifted only for `iceberg-spark-3.5_*`
  and `iceberg-spark-4.0_*` (because `ArrowInputPartitionContext` takes
  `com.google.common.collect.ImmutableList`).

---

## 3. Investigations

Each investigation lists the question, what we'd do with the answer, and search hints.
Answer with citations. If something can't be found, say so explicitly with what you
tried.

### Investigation 1 — `bq-advanced` table mode detection

**Question:** Given an `org.apache.iceberg.Table` instance on the Spark driver, what is
the canonical way to decide whether to route through `HybridSparkScan` (bq-advanced) or
the existing `SparkBatchQueryScan` (regular Iceberg)?

**Sub-questions:**

1. What's the canonical signal? Possible options:
   - An Iceberg table property (e.g. `bigquery.table.mode = advanced`).
   - A catalog plugin field — e.g. `BigQueryMetastoreCatalog` exposing a typed flag.
   - A BigQuery admin API call (`tables.get` returning a `BigLakeConfiguration` /
     `IcebergMode` enum).
   - An Arrow / file format hint embedded in metadata.
2. What does "bq-advanced" semantically include? Only Managed Iceberg tables with rows
   in Vortex? Also FGAC-protected tables? Any table where `GenerateScanPlan` is
   preferred?
3. Cost: is the predicate cheap (in-memory property read) or RPC-bound? If RPC, can it
   be cached per scan?
4. Is there already a Java helper in google3 / Dataproc that computes this same
   predicate? We want to reuse it, not re-derive.

**What we'll do with the answer:** implement a `boolean isBqAdvancedTable(Table)` helper
in the spark-3.5 and spark-4.0 modules and call it from `SparkScanBuilder.buildBatchScan()`
to dispatch to `HybridSparkScan`.

**Search hints:** `bq-advanced`, `IcebergMode`, `BigLake`, `ManagedIceberg`,
`isBqAdvanced`, `bigquery.table.mode`, internal Dataproc catalog plugins for Iceberg.

---

### Investigation 2 — `GenerateScanPlan` proto + Java client

**Question:** What does the `GenerateScanPlan` RPC look like, and how do we invoke it
from Java?

**Sub-questions:**

1. **Proto location.** Find the `.proto` file that defines
   `com.google.cloud.bigquery.storage.v1beta2.BigQueryRead.GenerateScanPlan`. Provide:
   - The path in google3 or in the open-source `google-cloud-bigquerystorage` source.
   - Verbatim message definitions for `GenerateScanPlanRequest` and
     `GenerateScanPlanResponse`.
2. **Response shape — file half.** What fields encode the GCS file tasks? Are we given:
   - Raw GCS URIs + `(start, length, file_format)` triples, or
   - Fully-formed Iceberg `DataFile` protos, or
   - Something else?

   We need this to translate the response into Iceberg's `FileScanTask` (likely via
   `BaseFileScanTask` constructed from a `DataFile`).
3. **Response shape — stream half.** Is it:
   - An embedded `com.google.cloud.bigquery.storage.v1.ReadSession` (i.e. the same
     proto type the BQ Storage Read API already uses), or
   - A different, hybrid-specific message?

   We need this to populate `BqStreamScanTask` (`streamName`, `readSessionName`,
   `serializedArrowSchema`, `selectedFields`, stats).
4. **Request fields.** Required vs optional. What does the request need from us:
   - Project / billing project / location?
   - Table reference (project.dataset.table)?
   - Projection (column list)?
   - Row restriction (SQL WHERE-style filter)?
   - Max parallelism (number of streams to return)?
   - Preferred response data format (Arrow vs Avro)?
   - Any session token / consistency snapshot reference?
5. **Java stub.** Does `google-cloud-bigquerystorage:3.22.1` (already on our classpath)
   include a generated `BigQueryReadClient.generateScanPlan(...)` method, or does the
   stub live in a separate library (Investigation 3)?
6. **Auth scope.** Does this RPC need a new OAuth scope beyond
   `https://www.googleapis.com/auth/bigquery`?

**What we'll do with the answer:** implement a real `BqAdvancedScanPlanner` that:
1. Calls `GenerateScanPlan` once per scan.
2. Splits the response into a `List<FileScanTask>` (passed to `HybridSparkScan` via the
   existing file `Scan`) and a `List<BqStreamScanTask>` (returned from
   `planStreams(...)`).

**Search hints:** `GenerateScanPlan` in google3, internal proto repos for
`bigquery.storage.v1beta2`, `BigQueryReadClient`, `HybridStream`, `ScanPlan`.

---

### Investigation 3 — Artifact registry coordinates

**Question:** What's the exact GAV (and AR repo URL) for the jar that provides the
`GenerateScanPlan` Java stub, if it isn't already in `google-cloud-bigquerystorage:3.22.1`?

**Sub-questions:**

1. **GAV.** `groupId:artifactId:version`. Likely candidates:
   - An internal-only fork of `google-cloud-bigquerystorage`.
   - A separate proto-stubs jar (e.g. `proto-google-cloud-bigquerystorage-v1beta2`).
   - A new artifact specific to the Hybrid API.
2. **AR repository URL.** What `repositories { maven { url '...' } }` block needs to be
   added to the root `build.gradle`?
3. **AR authentication.** How does Dataproc's build env authenticate to AR? Service
   account key, OAuth, gcloud credential helper, something else?
4. **Compatibility with existing pins.**
   - Does the AR jar's `google-cloud-bigquerystorage` version line up with the 3.22.1
     we already get transitively from the connector? If not, what conflicts arise and
     how should we resolve them (force the AR version, force the connector version,
     bump the connector)?
   - Does the AR jar bring more unshaded Guava? What version? Is it compatible with
     what the connector ships (Guava 33.5.0-jre)?
   - Any gRPC/Netty version skew vs `iceberg-arrow`?
   - Any protobuf version skew (the connector's parent pom pins protobuf around 3.x to
     stay compatible with bigquerystorage)?

**What we'll do with the answer:** add a `[versions]` + `[libraries]` entry in
`gradle/libs.versions.toml`, declare the AR repo in the root `build.gradle`, and add
the `implementation libs.<...>` line to `spark/v3.5/build.gradle` and
`spark/v4.0/build.gradle`. Configure CI auth.

**Search hints:** internal go/ for "artifact registry maven", "BigQuery Storage Read
API hybrid jar", `BUILD.bazel` files referencing `GenerateScanPlan`.

---

### Investigation 4 — Credential vending

**Question:** How are credentials supplied to executors for the BQ Storage Read API
gRPC reads? Two viable models:

- **Single-supplier:** the driver builds one `BigQueryClientFactory` with broadcast-able
  creds; the same factory is used for every executor partition. Simpler.
- **Per-stream tokens:** `GenerateScanPlanResponse` returns scoped tokens per stream;
  we put them on `BqStreamScanTask` and synthesize a per-task client on the executor.
  Stronger for FGAC.

**Sub-questions:**

1. Which model does the BQ team expect downstream consumers (Dataproc) to use?
2. **If single-supplier:**
   - Recommended `BigQueryCredentialsSupplier` configuration in a Dataproc Spark
     context. Service account key file path? Spark conf key
     (`spark.bigquery.credentials.<...>`)? Metadata server fallback?
     `ImpersonatedCredentials`?
   - Is there a Dataproc-specific helper (e.g. `DataprocBigQueryCredentialsSupplier`)
     that already wires the right thing?
3. **If per-stream tokens:**
   - Field name + type on `GenerateScanPlanResponse` carrying the token.
   - Token lifetime; refresh story (do we re-call `GenerateScanPlan` near expiry, or
     does the response include a refresh token, or do we abandon the partition?).
   - How does the connector's `BigQueryClientFactory` accept a per-call credential
     override? If it can't, do we need a thinner client (we'd hand-roll a
     `BigQueryReadClient` per task)?
   - Does `BqStreamScanTask` need a new `byte[] credentialToken` field (Slice 1 data
     model is locked — adding a field here is allowed but should be deliberate)?
4. **Cross-cutting:** does the GCS file half need separate credentials (for Iceberg's
   `FileIO`) vs the streaming half? Today they're unified at the Dataproc level — is
   that still true under bq-advanced?

**What we'll do with the answer:** either (a) document the recommended single-supplier
configuration and add wiring code in `SparkScanBuilder.buildBatchScan()`, or
(b) extend `BqStreamScanTask` + `HybridReaderConfig` with the per-stream token field
and update `BigQueryStreamColumnarReader.openDelegate()` to construct a per-task
factory.

**Search hints:** `BigQueryCredentialsSupplier`, `Dataproc credentials BigQuery`,
internal Dataproc Spark integration docs, FGAC token issuance for BigLake.

---

### Investigation 5 — Schema / field-ID mapping

**Question:** When `GenerateScanPlanResponse` carries an Arrow IPC schema for the
stream half, are columns labeled in a way that lets us resolve them to **Iceberg
field IDs**?

**Sub-questions:**

1. Are BQ Arrow columns tagged with an `iceberg.field-id` (or similar) entry in the
   Arrow `Field.metadata` map? Or via a sidecar mapping in the response?
2. **If yes:** what's the metadata key name, and does the connector's
   `ArrowSchemaConverter` already honor it (or do we need a wrapper)?
3. **If no:** what's the safe fallback?
   - Name-based resolution: is the BQ schema guaranteed to use the same column names
     as the Iceberg schema for `bq-advanced` tables? If yes, this is fine; document
     the contract.
   - A schema-ID lookup: are there parallel arrays in the response that map ordinals
     to Iceberg field IDs?
4. **Nested types.** Do struct/map/array round-trip cleanly through the connector's
   Arrow → Spark `ColumnVector` conversion? Any known type-mismatch pitfalls
   (timestamps, decimals, geography)?
5. **Existing precedent.** Is there an internal `ArrowSchemaConverter` extension or
   similar already solving this in google3 / an internal connector fork? If yes,
   surface it; we'd rather reuse than re-derive.

**What we'll do with the answer:** either (a) document the field-ID metadata contract
and add an assertion at `BigQueryStreamColumnarReader.openDelegate()` that checks for
the expected metadata, or (b) add a name-to-field-ID resolver invoked when
constructing the `BqStreamScanTask` on the driver.

**Search hints:** `ArrowSchemaConverter`, `iceberg.field-id`, BQ Storage v1
`ArrowSchema` proto comments, internal connector forks with
`field_id` references.

---

## 4. Reporting format

A single Markdown file. For each investigation:

```
## Investigation N — <title>

### Answer
<one or two paragraphs, with inline citations like
`google3/path/to/file.java:123` or `go/internal-doc-name`>

### Evidence
<short code/proto/doc excerpts that support the answer>

### Open questions / trade-offs
<bullet list of decisions the user must make before implementation>
```

Then a final section:

```
## Recommended Slice 4 task order

1. <work item>  — depends on: <prior items>; produces: <artifact>
2. ...
```

The task list should be detailed enough that the user can walk down it like a
checklist. Concrete examples of items:

- "Add `https://us-maven.pkg.dev/<...>` repo to root build.gradle's
  `allprojects { repositories { ... } }` block."
- "Pin `<groupId>:<artifactId>` to `<version>` in `gradle/libs.versions.toml`."
- "Implement `BqAdvancedTableDetector.isBqAdvanced(Table)` in
  `spark/v3.5/spark/src/main/java/.../source/`."
- "Mirror the same file in `spark/v4.0/spark/.../source/`."
- "Add a `bqPlanner != null` branch in `SparkScanBuilder.buildBatchScan()` that
  constructs `HybridSparkScan` instead of `SparkBatchQueryScan`."

---

## 5. Out of scope for this research

- Native Query Engine (Velox / NQE) integration. **Skip entirely.**
- Iceberg 1.11 REST `PlanTable` migration. **Note in passing if relevant; don't
  block on it.**
- Iceberg write side (HybridStream is read-only).
- Stream skew handling (`SplittableScanTask` for BQ streams).
- Code changes to the fork — this is a research-only output.

---

## 6. Length & quality target

3–5 pages of Markdown. Bullets > prose. Cite often. Internal go/-links and
google3 file paths are acceptable citations; the user has access. If something
is genuinely not findable, say so explicitly along with what you tried — that's
useful signal too.

If you encounter a question whose answer is "ask the BQ team," name the
likely team / mailing list / oncall rotation and what specifically to ask.

---

*End of brief.*
