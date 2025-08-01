/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableMetadataParser.CURRENT_SNAPSHOT_ID;
import static org.apache.iceberg.TableMetadataParser.FORMAT_VERSION;
import static org.apache.iceberg.TableMetadataParser.LAST_COLUMN_ID;
import static org.apache.iceberg.TableMetadataParser.LAST_UPDATED_MILLIS;
import static org.apache.iceberg.TableMetadataParser.LOCATION;
import static org.apache.iceberg.TableMetadataParser.PARTITION_SPEC;
import static org.apache.iceberg.TableMetadataParser.PROPERTIES;
import static org.apache.iceberg.TableMetadataParser.SCHEMA;
import static org.apache.iceberg.TableMetadataParser.SNAPSHOTS;
import static org.apache.iceberg.TestHelpers.MAX_FORMAT_VERSION;
import static org.apache.iceberg.TestHelpers.assertSameSchemaList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadata.SnapshotLogEntry;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestTableMetadata {
  private static final String TEST_LOCATION = "s3://bucket/test/location";

  private static final Schema TEST_SCHEMA =
      new Schema(
          7,
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));

  private static final long SEQ_NO = 34;
  private static final int LAST_ASSIGNED_COLUMN_ID = 3;

  private static final PartitionSpec SPEC_5 =
      PartitionSpec.builderFor(TEST_SCHEMA).withSpecId(5).build();
  private static final SortOrder SORT_ORDER_3 =
      SortOrder.builderFor(TEST_SCHEMA)
          .withOrderId(3)
          .asc("y", NullOrder.NULLS_FIRST)
          .desc(Expressions.bucket("z", 4), NullOrder.NULLS_LAST)
          .build();

  @TempDir private Path temp;

  public TableOperations ops = new LocalTableOperations(temp);

  @Test
  @SuppressWarnings("MethodLength")
  public void testJsonConversion() throws Exception {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0,
            previousSnapshotId,
            null,
            previousSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            7,
            manifestList,
            null,
            null,
            null);

    List<HistoryEntry> snapshotLog =
        ImmutableList.<HistoryEntry>builder()
            .add(
                new SnapshotLogEntry(
                    previousSnapshot.timestampMillis(), previousSnapshot.snapshotId()))
            .add(
                new SnapshotLogEntry(
                    currentSnapshot.timestampMillis(), currentSnapshot.snapshotId()))
            .build();

    Schema schema = new Schema(6, Types.NestedField.required(10, "x", Types.StringType.get()));

    Map<String, SnapshotRef> refs =
        ImmutableMap.of(
            "main", SnapshotRef.branchBuilder(currentSnapshotId).build(),
            "previous", SnapshotRef.tagBuilder(previousSnapshotId).build(),
            "test", SnapshotRef.branchBuilder(previousSnapshotId).build());

    List<StatisticsFile> statisticsFiles =
        ImmutableList.of(
            new GenericStatisticsFile(
                11L,
                "/some/stats/file.puffin",
                100,
                42,
                ImmutableList.of(
                    new GenericBlobMetadata(
                        "some-stats", 11L, 2, ImmutableList.of(4), ImmutableMap.of()))));

    List<PartitionStatisticsFile> partitionStatisticsFiles =
        ImmutableList.of(
            ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(11L)
                .path("/some/partition/stats/file.parquet")
                .fileSizeInBytes(42L)
                .build());

    TableMetadata expected =
        new TableMetadata(
            null,
            MAX_FORMAT_VERSION,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            SEQ_NO,
            System.currentTimeMillis(),
            3,
            7,
            ImmutableList.of(TEST_SCHEMA, schema),
            5,
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            3,
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            snapshotLog,
            ImmutableList.of(),
            refs,
            statisticsFiles,
            partitionStatisticsFiles,
            40,
            ImmutableList.of(),
            ImmutableList.of());

    String asJson = TableMetadataParser.toJson(expected);
    TableMetadata metadata = TableMetadataParser.fromJson(asJson);

    assertThat(metadata.formatVersion()).isEqualTo(expected.formatVersion());
    assertThat(metadata.uuid()).isEqualTo(expected.uuid());
    assertThat(metadata.location()).isEqualTo(expected.location());
    assertThat(metadata.lastSequenceNumber()).isEqualTo(expected.lastSequenceNumber());
    assertThat(metadata.lastColumnId()).isEqualTo(expected.lastColumnId());
    assertThat(metadata.currentSchemaId()).isEqualTo(expected.currentSchemaId());
    assertSameSchemaList(expected.schemas(), metadata.schemas());
    assertThat(metadata.spec().toString()).isEqualTo(expected.spec().toString());
    assertThat(metadata.defaultSpecId()).isEqualTo(expected.defaultSpecId());
    assertThat(metadata.specs()).isEqualTo(expected.specs());
    assertThat(metadata.lastAssignedPartitionId()).isEqualTo(expected.spec().lastAssignedFieldId());
    assertThat(metadata.defaultSortOrderId()).isEqualTo(expected.defaultSortOrderId());
    assertThat(metadata.sortOrder()).isEqualTo(expected.sortOrder());
    assertThat(metadata.sortOrders()).isEqualTo(expected.sortOrders());
    assertThat(metadata.properties()).isEqualTo(expected.properties());
    assertThat(metadata.snapshotLog()).isEqualTo(expected.snapshotLog());
    assertThat(metadata.currentSnapshot().snapshotId()).isEqualTo(currentSnapshotId);
    assertThat(metadata.currentSnapshot().parentId()).isEqualTo(previousSnapshotId);
    assertThat(metadata.currentSnapshot().allManifests(ops.io()))
        .isEqualTo(currentSnapshot.allManifests(ops.io()));
    assertThat(metadata.currentSnapshot().schemaId()).isEqualTo(7);
    assertThat(metadata.snapshot(previousSnapshotId).snapshotId()).isEqualTo(previousSnapshotId);
    assertThat(metadata.snapshot(previousSnapshotId).allManifests(ops.io()))
        .isEqualTo(previousSnapshot.allManifests(ops.io()));
    assertThat(metadata.snapshot(previousSnapshotId).schemaId()).isNull();
    assertThat(metadata.statisticsFiles()).isEqualTo(statisticsFiles);
    assertThat(metadata.partitionStatisticsFiles()).isEqualTo(partitionStatisticsFiles);
    assertThat(metadata.refs()).isEqualTo(refs);
    assertThat(metadata.nextRowId()).isEqualTo(expected.nextRowId());
  }

  @Test
  public void testBackwardCompat() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(TEST_SCHEMA).identity("x").withSpecId(6).build();
    SortOrder sortOrder = SortOrder.unsorted();
    Schema schema = new Schema(TableMetadata.INITIAL_SCHEMA_ID, TEST_SCHEMA.columns());

    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0,
            previousSnapshotId,
            null,
            previousSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    TableMetadata expected =
        new TableMetadata(
            null,
            1,
            null,
            TEST_LOCATION,
            0,
            System.currentTimeMillis(),
            3,
            TableMetadata.INITIAL_SCHEMA_ID,
            ImmutableList.of(schema),
            6,
            ImmutableList.of(spec),
            spec.lastAssignedFieldId(),
            TableMetadata.INITIAL_SORT_ORDER_ID,
            ImmutableList.of(sortOrder),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            0,
            ImmutableList.of(),
            ImmutableList.of());

    String asJson = toJsonWithoutSpecAndSchemaList(expected);
    TableMetadata metadata = TableMetadataParser.fromJson(asJson);

    assertThat(metadata.formatVersion()).isEqualTo(expected.formatVersion());
    assertThat(metadata.uuid()).as("Table UUID should not be assigned").isNull();
    assertThat(metadata.location()).isEqualTo(expected.location());
    assertThat(metadata.lastSequenceNumber())
        .as("Last sequence number should default to 0")
        .isEqualTo(expected.lastSequenceNumber());

    assertThat(metadata.lastColumnId()).isEqualTo(expected.lastColumnId());
    assertThat(metadata.currentSchemaId())
        .as("Current schema ID should be default to TableMetadata.INITIAL_SCHEMA_ID")
        .isEqualTo(TableMetadata.INITIAL_SCHEMA_ID);
    assertThat(metadata.schemas()).hasSize(1);
    assertThat(metadata.schemas().get(0).asStruct()).isEqualTo(schema.asStruct());
    assertThat(metadata.spec().toString()).isEqualTo(expected.spec().toString());
    assertThat(metadata.defaultSpecId()).isEqualTo(TableMetadata.INITIAL_SPEC_ID);
    assertThat(metadata.specs()).hasSize(1);
    assertThat(metadata.specs())
        .first()
        .satisfies(partitionSpec -> partitionSpec.compatibleWith(spec));
    assertThat(metadata.specs().get(0).specId()).isEqualTo(TableMetadata.INITIAL_SPEC_ID);
    assertThat(metadata.lastAssignedPartitionId()).isEqualTo(expected.spec().lastAssignedFieldId());
    assertThat(metadata.properties()).isEqualTo(expected.properties());
    assertThat(metadata.snapshotLog()).isEqualTo(expected.snapshotLog());
    assertThat(metadata.currentSnapshot().snapshotId()).isEqualTo(currentSnapshotId);
    assertThat(metadata.currentSnapshot().parentId()).isEqualTo(previousSnapshotId);
    assertThat(metadata.currentSnapshot().allManifests(ops.io()))
        .as("Current snapshot files should match")
        .isEqualTo(currentSnapshot.allManifests(ops.io()));
    assertThat(metadata.currentSnapshot().schemaId()).isNull();
    assertThat(metadata.snapshot(previousSnapshotId).snapshotId()).isEqualTo(previousSnapshotId);
    assertThat(metadata.snapshot(previousSnapshotId).allManifests(ops.io()))
        .isEqualTo(previousSnapshot.allManifests(ops.io()));
    assertThat(metadata.previousFiles()).isEqualTo(expected.previousFiles());
    assertThat(metadata.snapshot(previousSnapshotId).schemaId()).isNull();
    assertThat(metadata.nextRowId()).isEqualTo(expected.nextRowId());
  }

  @Test
  public void testInvalidMainBranch() throws IOException {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0,
            previousSnapshotId,
            null,
            previousSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");

    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            7,
            manifestList,
            null,
            null,
            null);

    List<HistoryEntry> snapshotLog =
        ImmutableList.<HistoryEntry>builder()
            .add(
                new SnapshotLogEntry(
                    previousSnapshot.timestampMillis(), previousSnapshot.snapshotId()))
            .add(
                new SnapshotLogEntry(
                    currentSnapshot.timestampMillis(), currentSnapshot.snapshotId()))
            .build();

    Schema schema = new Schema(6, Types.NestedField.required(10, "x", Types.StringType.get()));

    Map<String, SnapshotRef> refs =
        ImmutableMap.of("main", SnapshotRef.branchBuilder(previousSnapshotId).build());

    assertThatThrownBy(
            () ->
                new TableMetadata(
                    null,
                    MAX_FORMAT_VERSION,
                    UUID.randomUUID().toString(),
                    TEST_LOCATION,
                    SEQ_NO,
                    System.currentTimeMillis(),
                    3,
                    7,
                    ImmutableList.of(TEST_SCHEMA, schema),
                    5,
                    ImmutableList.of(SPEC_5),
                    SPEC_5.lastAssignedFieldId(),
                    3,
                    ImmutableList.of(SORT_ORDER_3),
                    ImmutableMap.of("property", "value"),
                    currentSnapshotId,
                    Arrays.asList(previousSnapshot, currentSnapshot),
                    null,
                    snapshotLog,
                    ImmutableList.of(),
                    refs,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    0L,
                    ImmutableList.of(),
                    ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Current snapshot ID does not match main branch");
  }

  @Test
  public void testMainWithoutCurrent() throws IOException {
    long snapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(snapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot snapshot =
        new BaseSnapshot(
            0, snapshotId, null, snapshotId, null, null, null, manifestList, null, null, null);

    Schema schema = new Schema(6, Types.NestedField.required(10, "x", Types.StringType.get()));

    Map<String, SnapshotRef> refs =
        ImmutableMap.of("main", SnapshotRef.branchBuilder(snapshotId).build());

    assertThatThrownBy(
            () ->
                new TableMetadata(
                    null,
                    MAX_FORMAT_VERSION,
                    UUID.randomUUID().toString(),
                    TEST_LOCATION,
                    SEQ_NO,
                    System.currentTimeMillis(),
                    3,
                    7,
                    ImmutableList.of(TEST_SCHEMA, schema),
                    5,
                    ImmutableList.of(SPEC_5),
                    SPEC_5.lastAssignedFieldId(),
                    3,
                    ImmutableList.of(SORT_ORDER_3),
                    ImmutableMap.of("property", "value"),
                    -1,
                    ImmutableList.of(snapshot),
                    null,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    refs,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    0L,
                    ImmutableList.of(),
                    ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Current snapshot is not set, but main branch exists");
  }

  @Test
  public void testBranchSnapshotMissing() {
    long snapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    Schema schema = new Schema(6, Types.NestedField.required(10, "x", Types.StringType.get()));

    Map<String, SnapshotRef> refs =
        ImmutableMap.of("main", SnapshotRef.branchBuilder(snapshotId).build());

    assertThatThrownBy(
            () ->
                new TableMetadata(
                    null,
                    MAX_FORMAT_VERSION,
                    UUID.randomUUID().toString(),
                    TEST_LOCATION,
                    SEQ_NO,
                    System.currentTimeMillis(),
                    3,
                    7,
                    ImmutableList.of(TEST_SCHEMA, schema),
                    5,
                    ImmutableList.of(SPEC_5),
                    SPEC_5.lastAssignedFieldId(),
                    3,
                    ImmutableList.of(SORT_ORDER_3),
                    ImmutableMap.of("property", "value"),
                    -1,
                    ImmutableList.of(),
                    null,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    refs,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    0L,
                    ImmutableList.of(),
                    ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith("does not exist in the existing snapshots list");
  }

  private static String toJsonWithoutSpecAndSchemaList(TableMetadata metadata) {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);

      generator.writeStartObject(); // start table metadata object

      generator.writeNumberField(FORMAT_VERSION, 1);
      generator.writeStringField(LOCATION, metadata.location());
      generator.writeNumberField(LAST_UPDATED_MILLIS, metadata.lastUpdatedMillis());
      generator.writeNumberField(LAST_COLUMN_ID, metadata.lastColumnId());

      // mimic an old writer by writing only schema and not the current ID or schema list
      generator.writeFieldName(SCHEMA);
      SchemaParser.toJson(metadata.schema().asStruct(), generator);

      // mimic an old writer by writing only partition-spec and not the default ID or spec list
      generator.writeFieldName(PARTITION_SPEC);
      PartitionSpecParser.toJsonFields(metadata.spec(), generator);

      JsonUtil.writeStringMap(PROPERTIES, metadata.properties(), generator);

      generator.writeNumberField(
          CURRENT_SNAPSHOT_ID,
          metadata.currentSnapshot() != null ? metadata.currentSnapshot().snapshotId() : -1);

      generator.writeArrayFieldStart(SNAPSHOTS);
      for (Snapshot snapshot : metadata.snapshots()) {
        SnapshotParser.toJson(snapshot, generator);
      }
      generator.writeEndArray();
      // skip the snapshot log

      generator.writeEndObject(); // end table metadata object

      generator.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write json for: %s", metadata), e);
    }
    return writer.toString();
  }

  @Test
  public void testJsonWithPreviousMetadataLog() throws Exception {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0,
            previousSnapshotId,
            null,
            previousSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp, "/tmp/000001-" + UUID.randomUUID() + ".metadata.json"));

    TableMetadata base =
        new TableMetadata(
            null,
            1,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            0,
            System.currentTimeMillis(),
            3,
            7,
            ImmutableList.of(TEST_SCHEMA),
            5,
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            3,
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            reversedSnapshotLog,
            ImmutableList.copyOf(previousMetadataLog),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            0L,
            ImmutableList.of(),
            ImmutableList.of());

    String asJson = TableMetadataParser.toJson(base);
    TableMetadata metadataFromJson = TableMetadataParser.fromJson(asJson);

    assertThat(metadataFromJson.previousFiles()).isEqualTo(previousMetadataLog);
  }

  @Test
  public void testAddPreviousMetadataRemoveNone() throws IOException {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0,
            previousSnapshotId,
            null,
            previousSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);
    long currentSnapshotId = System.currentTimeMillis();

    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    reversedSnapshotLog.add(
        new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshotId));
    reversedSnapshotLog.add(
        new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshotId));
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 100, "/tmp/000001-" + UUID.randomUUID() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 90, "/tmp/000002-" + UUID.randomUUID() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata =
        new MetadataLogEntry(
            currentTimestamp - 80, "/tmp/000003-" + UUID.randomUUID() + ".metadata.json");

    TableMetadata base =
        new TableMetadata(
            latestPreviousMetadata.file(),
            1,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            0,
            currentTimestamp - 80,
            3,
            7,
            ImmutableList.of(TEST_SCHEMA),
            5,
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            3,
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            reversedSnapshotLog,
            ImmutableList.copyOf(previousMetadataLog),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            0L,
            ImmutableList.of(),
            ImmutableList.of());

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata =
        base.replaceProperties(
            ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "5"));
    Set<MetadataLogEntry> removedPreviousMetadata = Sets.newHashSet(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    assertThat(metadata.previousFiles()).isEqualTo(previousMetadataLog);
    assertThat(removedPreviousMetadata).isEmpty();
  }

  @Test
  public void testAddPreviousMetadataRemoveOne() throws IOException {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0,
            previousSnapshotId,
            null,
            previousSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    reversedSnapshotLog.add(
        new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshotId));
    reversedSnapshotLog.add(
        new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshotId));
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 100, "/tmp/000001-" + UUID.randomUUID() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 90, "/tmp/000002-" + UUID.randomUUID() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 80, "/tmp/000003-" + UUID.randomUUID() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 70, "/tmp/000004-" + UUID.randomUUID() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 60, "/tmp/000005-" + UUID.randomUUID() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata =
        new MetadataLogEntry(
            currentTimestamp - 50, "/tmp/000006-" + UUID.randomUUID() + ".metadata.json");

    TableMetadata base =
        new TableMetadata(
            latestPreviousMetadata.file(),
            1,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            0,
            currentTimestamp - 50,
            3,
            7,
            ImmutableList.of(TEST_SCHEMA),
            5,
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            3,
            ImmutableList.of(SORT_ORDER_3),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            reversedSnapshotLog,
            ImmutableList.copyOf(previousMetadataLog),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            0L,
            ImmutableList.of(),
            ImmutableList.of());

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata =
        base.replaceProperties(
            ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "5"));

    SortedSet<MetadataLogEntry> removedPreviousMetadata =
        Sets.newTreeSet(Comparator.comparingLong(MetadataLogEntry::timestampMillis));
    removedPreviousMetadata.addAll(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    assertThat(metadata.previousFiles()).isEqualTo(previousMetadataLog.subList(1, 6));
    assertThat(ImmutableList.copyOf(removedPreviousMetadata))
        .isEqualTo(previousMetadataLog.subList(0, 1));
  }

  @Test
  public void testAddPreviousMetadataRemoveMultiple() throws IOException {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestList =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");
    Snapshot previousSnapshot =
        new BaseSnapshot(
            0,
            previousSnapshotId,
            null,
            previousSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    long currentSnapshotId = System.currentTimeMillis();
    manifestList =
        createManifestListWithManifestFile(
            currentSnapshotId, previousSnapshotId, "file:/tmp/manifest2.avro");
    Snapshot currentSnapshot =
        new BaseSnapshot(
            0,
            currentSnapshotId,
            previousSnapshotId,
            currentSnapshotId,
            null,
            null,
            null,
            manifestList,
            null,
            null,
            null);

    List<HistoryEntry> reversedSnapshotLog = Lists.newArrayList();
    reversedSnapshotLog.add(
        new SnapshotLogEntry(previousSnapshot.timestampMillis(), previousSnapshotId));
    reversedSnapshotLog.add(
        new SnapshotLogEntry(currentSnapshot.timestampMillis(), currentSnapshotId));
    long currentTimestamp = System.currentTimeMillis();
    List<MetadataLogEntry> previousMetadataLog = Lists.newArrayList();
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 100, "/tmp/000001-" + UUID.randomUUID() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 90, "/tmp/000002-" + UUID.randomUUID() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 80, "/tmp/000003-" + UUID.randomUUID() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 70, "/tmp/000004-" + UUID.randomUUID() + ".metadata.json"));
    previousMetadataLog.add(
        new MetadataLogEntry(
            currentTimestamp - 60, "/tmp/000005-" + UUID.randomUUID() + ".metadata.json"));

    MetadataLogEntry latestPreviousMetadata =
        new MetadataLogEntry(
            currentTimestamp - 50, "/tmp/000006-" + UUID.randomUUID() + ".metadata.json");

    TableMetadata base =
        new TableMetadata(
            latestPreviousMetadata.file(),
            1,
            UUID.randomUUID().toString(),
            TEST_LOCATION,
            0,
            currentTimestamp - 50,
            3,
            7,
            ImmutableList.of(TEST_SCHEMA),
            SPEC_5.specId(),
            ImmutableList.of(SPEC_5),
            SPEC_5.lastAssignedFieldId(),
            SortOrder.unsorted().orderId(),
            ImmutableList.of(SortOrder.unsorted()),
            ImmutableMap.of("property", "value"),
            currentSnapshotId,
            Arrays.asList(previousSnapshot, currentSnapshot),
            null,
            reversedSnapshotLog,
            ImmutableList.copyOf(previousMetadataLog),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            0L,
            ImmutableList.of(),
            ImmutableList.of());

    previousMetadataLog.add(latestPreviousMetadata);

    TableMetadata metadata =
        base.replaceProperties(
            ImmutableMap.of(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2"));

    SortedSet<MetadataLogEntry> removedPreviousMetadata =
        Sets.newTreeSet(Comparator.comparingLong(MetadataLogEntry::timestampMillis));
    removedPreviousMetadata.addAll(base.previousFiles());
    removedPreviousMetadata.removeAll(metadata.previousFiles());

    assertThat(metadata.previousFiles()).isEqualTo(previousMetadataLog.subList(4, 6));
    assertThat(ImmutableList.copyOf(removedPreviousMetadata))
        .isEqualTo(previousMetadataLog.subList(0, 4));
  }

  @Test
  public void testV2UUIDValidation() {
    assertThatThrownBy(
            () ->
                new TableMetadata(
                    null,
                    2,
                    null,
                    TEST_LOCATION,
                    SEQ_NO,
                    System.currentTimeMillis(),
                    LAST_ASSIGNED_COLUMN_ID,
                    7,
                    ImmutableList.of(TEST_SCHEMA),
                    SPEC_5.specId(),
                    ImmutableList.of(SPEC_5),
                    SPEC_5.lastAssignedFieldId(),
                    3,
                    ImmutableList.of(SORT_ORDER_3),
                    ImmutableMap.of(),
                    -1L,
                    ImmutableList.of(),
                    null,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    0L,
                    ImmutableList.of(),
                    ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("UUID is required in format v2");
  }

  @Test
  public void testVersionValidation() {
    int supportedVersion = TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION;
    int unsupportedVersion = supportedVersion + 1;
    assertThatThrownBy(
            () ->
                new TableMetadata(
                    null,
                    unsupportedVersion,
                    null,
                    TEST_LOCATION,
                    SEQ_NO,
                    System.currentTimeMillis(),
                    LAST_ASSIGNED_COLUMN_ID,
                    7,
                    ImmutableList.of(TEST_SCHEMA),
                    SPEC_5.specId(),
                    ImmutableList.of(SPEC_5),
                    SPEC_5.lastAssignedFieldId(),
                    3,
                    ImmutableList.of(SORT_ORDER_3),
                    ImmutableMap.of(),
                    -1L,
                    ImmutableList.of(),
                    null,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    0L,
                    ImmutableList.of(),
                    ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Unsupported format version: v%s (supported: v%s)",
            unsupportedVersion, supportedVersion);

    assertThatThrownBy(
            () ->
                TableMetadata.newTableMetadata(
                    TEST_SCHEMA,
                    PartitionSpec.unpartitioned(),
                    SortOrder.unsorted(),
                    TEST_LOCATION,
                    ImmutableMap.of(),
                    unsupportedVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Unsupported format version: v%s (supported: v%s)",
            unsupportedVersion, supportedVersion);

    // should be allowed in the supported version
    assertThat(
            new TableMetadata(
                null,
                supportedVersion,
                UUID.randomUUID().toString(),
                TEST_LOCATION,
                SEQ_NO,
                System.currentTimeMillis(),
                LAST_ASSIGNED_COLUMN_ID,
                7,
                ImmutableList.of(TEST_SCHEMA),
                SPEC_5.specId(),
                ImmutableList.of(SPEC_5),
                SPEC_5.lastAssignedFieldId(),
                3,
                ImmutableList.of(SORT_ORDER_3),
                ImmutableMap.of(),
                -1L,
                ImmutableList.of(),
                null,
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                0L,
                ImmutableList.of(),
                ImmutableList.of()))
        .isNotNull();

    assertThat(
            TableMetadata.newTableMetadata(
                TEST_SCHEMA,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                TEST_LOCATION,
                ImmutableMap.of(),
                supportedVersion))
        .isNotNull();
  }

  @Test
  public void testParserVersionValidation() throws Exception {
    String supportedVersion1 = readTableMetadataInputFile("TableMetadataV1Valid.json");
    TableMetadata parsed1 = TableMetadataParser.fromJson(supportedVersion1);
    assertThat(parsed1).as("Should successfully read supported metadata version").isNotNull();

    String supportedVersion2 = readTableMetadataInputFile("TableMetadataV2Valid.json");
    TableMetadata parsed2 = TableMetadataParser.fromJson(supportedVersion2);
    assertThat(parsed2).as("Should successfully read supported metadata version").isNotNull();

    String unsupportedVersion = readTableMetadataInputFile("TableMetadataUnsupportedVersion.json");
    assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupportedVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot read unsupported version");
  }

  @Test
  public void testParserV2PartitionSpecsValidation() throws Exception {
    String unsupportedVersion =
        readTableMetadataInputFile("TableMetadataV2MissingPartitionSpecs.json");
    assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupportedVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("partition-specs must exist in format v2");
  }

  @Test
  public void testParserV2LastAssignedFieldIdValidation() throws Exception {
    String unsupportedVersion =
        readTableMetadataInputFile("TableMetadataV2MissingLastPartitionId.json");
    assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupportedVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("last-partition-id must exist in format v2");
  }

  @Test
  public void testParserV2SortOrderValidation() throws Exception {
    String unsupportedVersion = readTableMetadataInputFile("TableMetadataV2MissingSortOrder.json");
    assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupportedVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("sort-orders must exist in format v2");
  }

  @Test
  public void testParserV2CurrentSchemaIdValidation() throws Exception {
    String unsupported = readTableMetadataInputFile("TableMetadataV2CurrentSchemaNotFound.json");
    assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupported))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find schema with current-schema-id=2 from schemas");
  }

  @Test
  public void testParserV2SchemasValidation() throws Exception {
    String unsupported = readTableMetadataInputFile("TableMetadataV2MissingSchemas.json");
    assertThatThrownBy(() -> TableMetadataParser.fromJson(unsupported))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("schemas must exist in format v2");
  }

  private String readTableMetadataInputFile(String fileName) throws Exception {
    Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
    return String.join("", java.nio.file.Files.readAllLines(path));
  }

  @Test
  public void testNewTableMetadataReassignmentAllIds() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(3, "x", Types.LongType.get()),
            Types.NestedField.required(4, "y", Types.LongType.get()),
            Types.NestedField.required(5, "z", Types.LongType.get()));

    PartitionSpec spec =
        PartitionSpec.builderFor(schema)
            .withSpecId(5)
            .add(3, 1005, "x_partition", Transforms.bucket(4))
            .add(5, 1003, "z_partition", Transforms.bucket(8))
            .build();
    String location = "file://tmp/db/table";
    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, spec, location, ImmutableMap.of());

    // newTableMetadata should reassign column ids and partition field ids.
    PartitionSpec expected =
        PartitionSpec.builderFor(metadata.schema())
            .withSpecId(0)
            .add(1, 1000, "x_partition", Transforms.bucket(4))
            .add(3, 1001, "z_partition", Transforms.bucket(8))
            .build();

    assertThat(metadata.spec()).isEqualTo(expected);
  }

  @Test
  public void testInvalidUpdatePartitionSpecForV1Table() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "x", Types.LongType.get()));

    PartitionSpec spec =
        PartitionSpec.builderFor(schema)
            .withSpecId(5)
            .add(1, 1005, "x_partition", Transforms.bucket(4))
            .build();
    String location = "file://tmp/db/table";
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            location,
            ImmutableMap.of(),
            1);

    assertThatThrownBy(() -> metadata.updatePartitionSpec(spec))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Spec does not use sequential IDs that are required in v1");
  }

  @Test
  public void testBuildReplacementForV1Table() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "x", Types.LongType.get()),
            Types.NestedField.required(2, "y", Types.LongType.get()));
    PartitionSpec spec =
        PartitionSpec.builderFor(schema).withSpecId(0).identity("x").identity("y").build();
    String location = "file://tmp/db/table";
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, spec, SortOrder.unsorted(), location, ImmutableMap.of(), 1);
    assertThat(metadata.spec()).isEqualTo(spec);

    Schema updatedSchema =
        new Schema(
            Types.NestedField.required(1, "x", Types.LongType.get()),
            Types.NestedField.required(2, "z", Types.StringType.get()),
            Types.NestedField.required(3, "y", Types.LongType.get()));
    PartitionSpec updatedSpec =
        PartitionSpec.builderFor(updatedSchema).withSpecId(0).bucket("z", 8).identity("x").build();
    TableMetadata updated =
        metadata.buildReplacement(
            updatedSchema, updatedSpec, SortOrder.unsorted(), location, ImmutableMap.of());
    PartitionSpec expected =
        PartitionSpec.builderFor(updated.schema())
            .withSpecId(1)
            .add(1, 1000, "x", Transforms.identity())
            .add(2, 1001, "y", Transforms.alwaysNull())
            .add(3, 1002, "z_bucket", Transforms.bucket(8))
            .build();
    assertThat(updated.spec())
        .as(
            "Should reassign the partition field IDs and reuse any existing IDs for equivalent fields")
        .isEqualTo(expected);
  }

  @Test
  public void testBuildReplacementForV2Table() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "x", Types.LongType.get()),
            Types.NestedField.required(2, "y", Types.LongType.get()));
    PartitionSpec spec =
        PartitionSpec.builderFor(schema).withSpecId(0).identity("x").identity("y").build();
    String location = "file://tmp/db/table";
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, spec, SortOrder.unsorted(), location, ImmutableMap.of(), 2);
    assertThat(metadata.spec()).isEqualTo(spec);

    Schema updatedSchema =
        new Schema(
            Types.NestedField.required(1, "x", Types.LongType.get()),
            Types.NestedField.required(2, "z", Types.StringType.get()));
    PartitionSpec updatedSpec =
        PartitionSpec.builderFor(updatedSchema).withSpecId(0).bucket("z", 8).identity("x").build();
    TableMetadata updated =
        metadata.buildReplacement(
            updatedSchema, updatedSpec, SortOrder.unsorted(), location, ImmutableMap.of());
    PartitionSpec expected =
        PartitionSpec.builderFor(updated.schema())
            .withSpecId(1)
            .add(3, 1002, "z_bucket", Transforms.bucket(8))
            .add(1, 1000, "x", Transforms.identity())
            .build();
    assertThat(updated.spec())
        .as(
            "Should reassign the partition field IDs and reuse any existing IDs for equivalent fields")
        .isEqualTo(expected);
  }

  @Test
  public void testSortOrder() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());
    assertThat(meta.sortOrder().isUnsorted()).isTrue();
    assertThat(meta.replaceSortOrder(SortOrder.unsorted()))
        .as("Should detect identical unsorted order")
        .isSameAs(meta);
  }

  @Test
  public void testUpdateSortOrder() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    SortOrder order = SortOrder.builderFor(schema).asc("x").build();

    TableMetadata sortedByX =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), order, null, ImmutableMap.of());
    assertThat(sortedByX.sortOrders()).hasSize(1);
    assertThat(sortedByX.sortOrder().orderId()).isEqualTo(1);
    assertThat(sortedByX.sortOrder().fields()).hasSize(1);
    assertThat(sortedByX.sortOrder().fields().get(0).sourceId()).isEqualTo(1);
    assertThat(sortedByX.sortOrder().fields().get(0).direction()).isEqualTo(SortDirection.ASC);
    assertThat(sortedByX.sortOrder().fields().get(0).nullOrder()).isEqualTo(NullOrder.NULLS_FIRST);

    // build an equivalent order with the correct schema
    SortOrder newOrder = SortOrder.builderFor(sortedByX.schema()).asc("x").build();

    TableMetadata alsoSortedByX = sortedByX.replaceSortOrder(newOrder);
    assertThat(sortedByX)
        .as("Should detect current sortOrder and not update")
        .isSameAs(alsoSortedByX);

    TableMetadata unsorted = alsoSortedByX.replaceSortOrder(SortOrder.unsorted());
    assertThat(unsorted.sortOrders()).hasSize(2);
    assertThat(unsorted.sortOrder().orderId()).isEqualTo(0);
    assertThat(unsorted.sortOrder().isUnsorted()).isTrue();

    TableMetadata sortedByXDesc =
        unsorted.replaceSortOrder(SortOrder.builderFor(unsorted.schema()).desc("x").build());
    assertThat(sortedByXDesc.sortOrders()).hasSize(3);
    assertThat(sortedByXDesc.sortOrder().orderId()).isEqualTo(2);
    assertThat(sortedByXDesc.sortOrder().fields()).hasSize(1);
    assertThat(sortedByXDesc.sortOrder().fields().get(0).sourceId()).isEqualTo(1);
    assertThat(sortedByXDesc.sortOrder().fields().get(0).direction()).isEqualTo(SortDirection.DESC);
    assertThat(sortedByX.sortOrder().fields().get(0).nullOrder()).isEqualTo(NullOrder.NULLS_FIRST);
  }

  @Test
  public void testStatistics() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());
    assertThat(meta.statisticsFiles()).as("Should default to no statistics files").isEmpty();
  }

  @Test
  public void testSetStatistics() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());

    TableMetadata withStatistics =
        TableMetadata.buildFrom(meta)
            .setStatistics(
                new GenericStatisticsFile(
                    43, "/some/path/to/stats/file", 128, 27, ImmutableList.of()))
            .build();

    assertThat(withStatistics.statisticsFiles())
        .as("There should be one statistics file registered")
        .hasSize(1);
    StatisticsFile statisticsFile = Iterables.getOnlyElement(withStatistics.statisticsFiles());
    assertThat(statisticsFile.snapshotId()).isEqualTo(43L);
    assertThat(statisticsFile.path()).isEqualTo("/some/path/to/stats/file");

    TableMetadata withStatisticsReplaced =
        TableMetadata.buildFrom(withStatistics)
            .setStatistics(
                new GenericStatisticsFile(
                    43, "/some/path/to/stats/file2", 128, 27, ImmutableList.of()))
            .build();

    assertThat(withStatisticsReplaced.statisticsFiles())
        .as("There should be one statistics file registered")
        .hasSize(1);
    statisticsFile = Iterables.getOnlyElement(withStatisticsReplaced.statisticsFiles());
    assertThat(statisticsFile.snapshotId()).isEqualTo(43L);
    assertThat(statisticsFile.path()).isEqualTo("/some/path/to/stats/file2");
  }

  @Test
  public void testRemoveStatistics() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.buildFrom(
                TableMetadata.newTableMetadata(
                    schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of()))
            .setStatistics(
                new GenericStatisticsFile(
                    43, "/some/path/to/stats/file", 128, 27, ImmutableList.of()))
            .setStatistics(
                new GenericStatisticsFile(
                    44, "/some/path/to/stats/file2", 128, 27, ImmutableList.of()))
            .build();

    assertThat(TableMetadata.buildFrom(meta).removeStatistics(42L).build())
        .as("Should detect no statistics to remove")
        .isSameAs(meta);

    TableMetadata withOneRemoved = TableMetadata.buildFrom(meta).removeStatistics(43).build();

    assertThat(withOneRemoved.statisticsFiles())
        .as("There should be one statistics file retained")
        .hasSize(1);
    StatisticsFile statisticsFile = Iterables.getOnlyElement(withOneRemoved.statisticsFiles());
    assertThat(statisticsFile.snapshotId()).isEqualTo(44L);
    assertThat(statisticsFile.path()).isEqualTo("/some/path/to/stats/file2");
  }

  @Test
  public void testPartitionStatistics() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());
    assertThat(meta.partitionStatisticsFiles())
        .as("Should default to no partition statistics files")
        .isEmpty();
  }

  @Test
  public void testSetPartitionStatistics() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());

    TableMetadata withPartitionStatistics =
        TableMetadata.buildFrom(meta)
            .setPartitionStatistics(
                ImmutableGenericPartitionStatisticsFile.builder()
                    .snapshotId(43)
                    .path("/some/path/to/partition/stats/file" + ".parquet")
                    .fileSizeInBytes(42L)
                    .build())
            .build();

    assertThat(withPartitionStatistics.partitionStatisticsFiles())
        .as("There should be one partition statistics file registered")
        .hasSize(1);
    PartitionStatisticsFile partitionStatisticsFile =
        Iterables.getOnlyElement(withPartitionStatistics.partitionStatisticsFiles());
    assertThat(partitionStatisticsFile.snapshotId()).isEqualTo(43L);
    assertThat(partitionStatisticsFile.path())
        .isEqualTo("/some/path/to/partition/stats/file.parquet");
    assertThat(partitionStatisticsFile.fileSizeInBytes()).isEqualTo(42L);

    TableMetadata withStatisticsReplaced =
        TableMetadata.buildFrom(withPartitionStatistics)
            .setPartitionStatistics(
                ImmutableGenericPartitionStatisticsFile.builder()
                    .snapshotId(43)
                    .path("/some/path/to/partition/stats/file2" + ".parquet")
                    .fileSizeInBytes(48L)
                    .build())
            .build();

    assertThat(withStatisticsReplaced.partitionStatisticsFiles())
        .as("There should be one statistics file registered")
        .hasSize(1);
    partitionStatisticsFile =
        Iterables.getOnlyElement(withStatisticsReplaced.partitionStatisticsFiles());
    assertThat(partitionStatisticsFile.snapshotId()).isEqualTo(43L);
    assertThat(partitionStatisticsFile.path())
        .isEqualTo("/some/path/to/partition/stats/file2.parquet");
    assertThat(partitionStatisticsFile.fileSizeInBytes()).isEqualTo(48L);
  }

  @Test
  public void testRemovePartitionStatistics() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.buildFrom(
                TableMetadata.newTableMetadata(
                    schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of()))
            .setPartitionStatistics(
                ImmutableGenericPartitionStatisticsFile.builder()
                    .snapshotId(43)
                    .path("/some/path/to/partition/stats/file1" + ".parquet")
                    .fileSizeInBytes(48L)
                    .build())
            .setPartitionStatistics(
                ImmutableGenericPartitionStatisticsFile.builder()
                    .snapshotId(44)
                    .path("/some/path/to/partition/stats/file2" + ".parquet")
                    .fileSizeInBytes(49L)
                    .build())
            .build();

    assertThat(TableMetadata.buildFrom(meta).removePartitionStatistics(42L).build())
        .as("Should detect no partition statistics to remove")
        .isSameAs(meta);

    TableMetadata withOneRemoved =
        TableMetadata.buildFrom(meta).removePartitionStatistics(43).build();

    assertThat(withOneRemoved.partitionStatisticsFiles())
        .as("There should be one partition statistics file retained")
        .hasSize(1);
    PartitionStatisticsFile partitionStatisticsFile =
        Iterables.getOnlyElement(withOneRemoved.partitionStatisticsFiles());
    assertThat(partitionStatisticsFile.snapshotId()).isEqualTo(44L);
    assertThat(partitionStatisticsFile.path())
        .isEqualTo("/some/path/to/partition/stats/file2.parquet");
    assertThat(partitionStatisticsFile.fileSizeInBytes()).isEqualTo(49L);
  }

  @Test
  public void testParseSchemaIdentifierFields() throws Exception {
    String data = readTableMetadataInputFile("TableMetadataV2Valid.json");
    TableMetadata parsed = TableMetadataParser.fromJson(data);
    assertThat(parsed.schemasById().get(0).identifierFieldIds()).isEmpty();
    assertThat(parsed.schemasById().get(1).identifierFieldIds()).containsExactly(1, 2);
  }

  @Test
  public void testParseMinimal() throws Exception {
    String data = readTableMetadataInputFile("TableMetadataV2ValidMinimal.json");
    TableMetadata parsed = TableMetadataParser.fromJson(data);
    assertThat(parsed.snapshots()).isEmpty();
    assertThat(parsed.snapshotLog()).isEmpty();
    assertThat(parsed.properties()).isEmpty();
    assertThat(parsed.previousFiles()).isEmpty();
  }

  @Test
  public void testUpdateSchemaIdentifierFields() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());

    Schema newSchema =
        new Schema(
            Lists.newArrayList(Types.NestedField.required(1, "x", Types.StringType.get())),
            Sets.newHashSet(1));
    TableMetadata newMeta = meta.updateSchema(newSchema);
    assertThat(newMeta.schemas()).hasSize(2);
    assertThat(newMeta.schema().identifierFieldIds()).containsExactly(1);
  }

  @Test
  public void testUpdateSchema() {
    Schema schema =
        new Schema(0, Types.NestedField.required(1, "y", Types.LongType.get(), "comment"));
    TableMetadata freshTable =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, ImmutableMap.of());
    assertThat(freshTable.currentSchemaId()).isEqualTo(TableMetadata.INITIAL_SCHEMA_ID);
    assertSameSchemaList(ImmutableList.of(schema), freshTable.schemas());
    assertThat(freshTable.schema().asStruct()).isEqualTo(schema.asStruct());
    assertThat(freshTable.lastColumnId()).isEqualTo(1);

    // update schema
    Schema schema2 =
        new Schema(
            Types.NestedField.required(1, "y", Types.LongType.get(), "comment"),
            Types.NestedField.required(2, "x", Types.StringType.get()));
    TableMetadata twoSchemasTable = freshTable.updateSchema(schema2);
    assertThat(twoSchemasTable.currentSchemaId()).isEqualTo(1);
    assertSameSchemaList(
        ImmutableList.of(schema, new Schema(1, schema2.columns())), twoSchemasTable.schemas());
    assertThat(twoSchemasTable.schema().asStruct()).isEqualTo(schema2.asStruct());
    assertThat(twoSchemasTable.lastColumnId()).isEqualTo(2);

    // update schema with the same schema and last column ID as current shouldn't cause change
    Schema sameSchema2 =
        new Schema(
            Types.NestedField.required(1, "y", Types.LongType.get(), "comment"),
            Types.NestedField.required(2, "x", Types.StringType.get()));
    TableMetadata sameSchemaTable = twoSchemasTable.updateSchema(sameSchema2);
    assertThat(sameSchemaTable).isSameAs(twoSchemasTable);

    // update schema with the same schema and different last column ID as current should create
    // a new table
    TableMetadata differentColumnIdTable = sameSchemaTable.updateSchema(sameSchema2);
    assertThat(differentColumnIdTable.currentSchemaId()).isEqualTo(1);
    assertSameSchemaList(
        ImmutableList.of(schema, new Schema(1, schema2.columns())),
        differentColumnIdTable.schemas());
    assertThat(differentColumnIdTable.schema().asStruct()).isEqualTo(schema2.asStruct());
    assertThat(differentColumnIdTable.lastColumnId()).isEqualTo(2);

    // update schema with old schema does not change schemas
    TableMetadata revertSchemaTable = differentColumnIdTable.updateSchema(schema);
    assertThat(revertSchemaTable.currentSchemaId()).isEqualTo(0);
    assertSameSchemaList(
        ImmutableList.of(schema, new Schema(1, schema2.columns())), revertSchemaTable.schemas());
    assertThat(revertSchemaTable.schema().asStruct()).isEqualTo(schema.asStruct());
    assertThat(revertSchemaTable.lastColumnId()).isEqualTo(2);

    // create new schema will use the largest schema id + 1
    Schema schema3 =
        new Schema(
            Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
            Types.NestedField.required(4, "x", Types.StringType.get()),
            Types.NestedField.required(6, "z", Types.IntegerType.get()));
    TableMetadata threeSchemaTable = revertSchemaTable.updateSchema(schema3);
    assertThat(threeSchemaTable.currentSchemaId()).isEqualTo(2);
    assertSameSchemaList(
        ImmutableList.of(
            schema, new Schema(1, schema2.columns()), new Schema(2, schema3.columns())),
        threeSchemaTable.schemas());
    assertThat(threeSchemaTable.schema().asStruct()).isEqualTo(schema3.asStruct());
    assertThat(threeSchemaTable.lastColumnId()).isEqualTo(6);
  }

  @Test
  public void testCreateV2MetadataThroughTableProperty() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2", "key", "val"));

    assertThat(meta.formatVersion()).isEqualTo(2);
    assertThat(meta.properties())
        .containsEntry("key", "val")
        .doesNotContainKey(TableProperties.FORMAT_VERSION);
  }

  @Test
  public void testManifestListIndexProjection() throws IOException {
    long previousSnapshotId = System.currentTimeMillis() - new Random(1234).nextInt(3600);

    String manifestListLocation =
        createManifestListWithManifestFile(previousSnapshotId, null, "file:/tmp/manifest1.avro");

    Schema manifestProjection =
        ManifestFile.schema()
            .select(
                ManifestFile.PATH.name(),
                ManifestFile.LENGTH.name(),
                ManifestFile.SPEC_ID.name(),
                ManifestFile.SNAPSHOT_ID.name());

    try (CloseableIterable<ManifestFile> manifestIterable =
        InternalData.read(FileFormat.AVRO, localInput(manifestListLocation))
            .setRootType(GenericManifestFile.class)
            .project(manifestProjection)
            .reuseContainers()
            .build()) {

      List<ManifestFile> manifests = Lists.newArrayList(manifestIterable);
      assertThat(manifests).hasSize(1);

      manifests.forEach(
          manifest -> {
            // projected fields
            assertThat(manifest)
                .extracting(
                    ManifestFile::path,
                    ManifestFile::length,
                    ManifestFile::partitionSpecId,
                    ManifestFile::snapshotId)
                .doesNotContainNull();

            // not projected with defaults
            assertThat(manifest.sequenceNumber()).isEqualTo(0);
            assertThat(manifest.minSequenceNumber()).isEqualTo(0);
            assertThat(manifest.content()).isEqualTo(ManifestContent.DATA);

            // not projected
            assertThat(manifest)
                .extracting(
                    ManifestFile::addedFilesCount,
                    ManifestFile::existingFilesCount,
                    ManifestFile::deletedFilesCount,
                    ManifestFile::addedRowsCount,
                    ManifestFile::existingRowsCount,
                    ManifestFile::deletedRowsCount,
                    ManifestFile::partitions,
                    ManifestFile::keyMetadata)
                .containsOnlyNulls();
          });
    }
  }

  private static Stream<Arguments> upgradeFormatVersionProvider() {
    // return a stream of all valid upgrade paths
    return IntStream.range(1, TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION)
        .boxed()
        .flatMap(
            baseFormatVersion ->
                IntStream.rangeClosed(
                        baseFormatVersion + 1, TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION)
                    .mapToObj(newFormatVersion -> arguments(baseFormatVersion, newFormatVersion)));
  }

  @ParameterizedTest
  @MethodSource("upgradeFormatVersionProvider")
  public void testReplaceMetadataThroughTableProperty(int baseFormatVersion, int newFormatVersion) {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, String.valueOf(baseFormatVersion), "key", "val"));

    meta =
        meta.buildReplacement(
            meta.schema(),
            meta.spec(),
            meta.sortOrder(),
            meta.location(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, String.valueOf(newFormatVersion), "key2", "val2"));

    assertThat(meta.formatVersion()).isEqualTo(newFormatVersion);
    assertThat(meta.properties())
        .containsEntry("key", "val")
        .containsEntry("key2", "val2")
        .doesNotContainKey(TableProperties.FORMAT_VERSION);
  }

  @ParameterizedTest
  @MethodSource("upgradeFormatVersionProvider")
  public void testUpgradeMetadataThroughTableProperty(int baseFormatVersion, int newFormatVersion) {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    TableMetadata meta =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, String.valueOf(baseFormatVersion), "key", "val"));

    meta =
        meta.replaceProperties(
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, String.valueOf(newFormatVersion), "key2", "val2"));

    assertThat(meta.formatVersion())
        .as("format version should be configured based on the format-version key")
        .isEqualTo(newFormatVersion);
    assertThat(meta.properties())
        .as("should not contain format-version but should contain new properties")
        .containsExactly(entry("key2", "val2"));
  }

  @Test
  public void testParseStatisticsFiles() throws Exception {
    String data = readTableMetadataInputFile("TableMetadataStatisticsFiles.json");
    TableMetadata parsed = TableMetadataParser.fromJson(data);
    assertThat(parsed.statisticsFiles()).hasSize(1);
    assertThat(parsed.statisticsFiles())
        .hasSize(1)
        .first()
        .isEqualTo(
            new GenericStatisticsFile(
                3055729675574597004L,
                "s3://a/b/stats.puffin",
                413,
                42,
                ImmutableList.of(
                    new GenericBlobMetadata(
                        "ndv", 3055729675574597004L, 1, ImmutableList.of(1), ImmutableMap.of()))));
  }

  @Test
  public void testParsePartitionStatisticsFiles() throws Exception {
    String data = readTableMetadataInputFile("TableMetadataPartitionStatisticsFiles.json");
    TableMetadata parsed = TableMetadataParser.fromJson(data);
    assertThat(parsed.partitionStatisticsFiles())
        .hasSize(1)
        .first()
        .isEqualTo(
            ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(3055729675574597004L)
                .path("s3://a/b/partition-stats.parquet")
                .fileSizeInBytes(43L)
                .build());
  }

  @Test
  public void testNoReservedPropertyForTableMetadataCreation() {
    Schema schema = new Schema(Types.NestedField.required(10, "x", Types.StringType.get()));

    assertThatThrownBy(
            () ->
                TableMetadata.newTableMetadata(
                    schema,
                    PartitionSpec.unpartitioned(),
                    null,
                    "/tmp",
                    ImmutableMap.of(TableProperties.FORMAT_VERSION, "1"),
                    1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Table properties should not contain reserved properties, but got {format-version=1}");

    assertThatThrownBy(
            () ->
                TableMetadata.newTableMetadata(
                    schema,
                    PartitionSpec.unpartitioned(),
                    null,
                    "/tmp",
                    ImmutableMap.of(TableProperties.UUID, "uuid"),
                    1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table properties should not contain reserved properties, but got {uuid=uuid}");
  }

  @Test
  public void testNoTrailingLocationSlash() {
    String locationWithSlash = "/with_trailing_slash/";
    String locationWithoutSlash = "/with_trailing_slash";
    TableMetadata meta =
        TableMetadata.newTableMetadata(
            TEST_SCHEMA, SPEC_5, SORT_ORDER_3, locationWithSlash, Collections.emptyMap());
    assertThat(meta.location())
        .as("Metadata should never return a location ending in a slash")
        .isEqualTo(locationWithoutSlash);
  }

  private String createManifestListWithManifestFile(
      long snapshotId, Long parentSnapshotId, String manifestFile) throws IOException {
    File manifestList = temp.resolve("manifests-" + System.nanoTime()).toFile();

    try (ManifestListWriter writer =
        ManifestLists.write(
            1, Files.localOutput(manifestList), snapshotId, parentSnapshotId, 0, 0L)) {
      writer.addAll(
          ImmutableList.of(
              new GenericManifestFile(localInput(manifestFile), SPEC_5.specId(), snapshotId)));
    }

    return localInput(manifestList).location();
  }

  @Test
  public void buildReplacementKeepsSnapshotLog() throws Exception {
    TableMetadata metadata =
        TableMetadataParser.fromJson(readTableMetadataInputFile("TableMetadataV2Valid.json"));
    assertThat(metadata.currentSnapshot()).isNotNull();
    assertThat(metadata.snapshots()).hasSize(2);
    assertThat(metadata.snapshotLog()).hasSize(2);

    TableMetadata replacement =
        metadata.buildReplacement(
            metadata.schema(),
            metadata.spec(),
            metadata.sortOrder(),
            metadata.location(),
            metadata.properties());

    assertThat(replacement.currentSnapshot()).isNull();
    assertThat(replacement.snapshots()).hasSize(2).containsExactlyElementsOf(metadata.snapshots());
    assertThat(replacement.snapshotLog())
        .hasSize(2)
        .containsExactlyElementsOf(metadata.snapshotLog());
  }

  @Test
  public void removeRefKeepsSnapshotLog() throws Exception {
    TableMetadata metadata =
        TableMetadataParser.fromJson(readTableMetadataInputFile("TableMetadataV2Valid.json"));
    assertThat(metadata.currentSnapshot()).isNotNull();
    assertThat(metadata.snapshots()).hasSize(2);
    assertThat(metadata.snapshotLog()).hasSize(2);

    TableMetadata removeRef =
        TableMetadata.buildFrom(metadata).removeRef(SnapshotRef.MAIN_BRANCH).build();

    assertThat(removeRef.currentSnapshot()).isNull();
    assertThat(removeRef.snapshots()).hasSize(2).containsExactlyElementsOf(metadata.snapshots());
    assertThat(removeRef.snapshotLog())
        .hasSize(2)
        .containsExactlyElementsOf(metadata.snapshotLog());
  }

  @Test
  public void testConstructV3Metadata() {
    TableMetadata.newTableMetadata(
        TEST_SCHEMA,
        PartitionSpec.unpartitioned(),
        SortOrder.unsorted(),
        TEST_LOCATION,
        ImmutableMap.of(),
        3);
  }

  @Test
  public void onlyMetadataLocationIsUpdatedWithoutTimestampAndMetadataLogEntry() {
    String uuid = "386b9f01-002b-4d8c-b77f-42c3fd3b7c9b";
    TableMetadata metadata =
        TableMetadata.buildFromEmpty()
            .assignUUID(uuid)
            .setLocation("location")
            .setCurrentSchema(TEST_SCHEMA, 3)
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .discardChanges()
            .withMetadataLocation("original-metadata-location")
            .build();

    assertThat(metadata.previousFiles()).isEmpty();
    assertThat(metadata.metadataFileLocation()).isEqualTo("original-metadata-location");

    // this will only update the metadata location without writing a new metadata log entry or
    // updating lastUpdatedMillis
    TableMetadata newMetadata =
        TableMetadata.buildFrom(metadata).withMetadataLocation("new-metadata-location").build();
    assertThat(newMetadata.lastUpdatedMillis()).isEqualTo(metadata.lastUpdatedMillis());
    assertThat(newMetadata.metadataFileLocation()).isEqualTo("new-metadata-location");
    assertThat(newMetadata.previousFiles()).isEmpty();

    TableMetadata updatedMetadata =
        TableMetadata.buildFrom(newMetadata)
            .withMetadataLocation("updated-metadata-location")
            .build();
    assertThat(updatedMetadata.lastUpdatedMillis()).isEqualTo(newMetadata.lastUpdatedMillis());
    assertThat(updatedMetadata.metadataFileLocation()).isEqualTo("updated-metadata-location");
    assertThat(updatedMetadata.previousFiles()).isEmpty();
  }

  @Test
  public void testMetadataWithRemoveSpecs() {
    TableMetadata meta =
        TableMetadata.buildFrom(
                TableMetadata.newTableMetadata(
                    TestBase.SCHEMA, PartitionSpec.unpartitioned(), null, ImmutableMap.of()))
            .removeSpecs(Sets.newHashSet())
            .build();

    assertThat(meta.changes()).noneMatch(u -> u instanceof MetadataUpdate.RemovePartitionSpecs);

    meta = TableMetadata.buildFrom(meta).removeSpecs(Sets.newHashSet(1, 2)).build();

    assertThat(meta.changes()).anyMatch(u -> u instanceof MetadataUpdate.RemovePartitionSpecs);
  }

  @Test
  public void testMetadataWithRemoveSchemas() {
    TableMetadata meta =
        TableMetadata.buildFrom(
                TableMetadata.newTableMetadata(
                    TestBase.SCHEMA, PartitionSpec.unpartitioned(), null, ImmutableMap.of()))
            .removeSchemas(Sets.newHashSet())
            .build();

    assertThat(meta.changes()).noneMatch(u -> u instanceof MetadataUpdate.RemoveSchemas);

    meta = TableMetadata.buildFrom(meta).removeSchemas(Sets.newHashSet(1, 2)).build();

    assertThat(meta.changes()).anyMatch(u -> u instanceof MetadataUpdate.RemoveSchemas);
  }
}
