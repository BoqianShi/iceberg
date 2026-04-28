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

/**
 * A {@link SparkScan} that emits a heterogeneous mix of {@link FileScanTask}s (read from GCS via
 * Iceberg's standard {@code FileIO} path) and {@link BqStreamScanTask}s (read from the BigQuery
 * Storage Read API). This is the driver-side half of the HybridStream integration; the executor
 * half lives in {@code HybridColumnarReaderFactory} (Slice 3, not yet implemented).
 *
 * <p>Each task type is bin-packed independently into homogeneous task groups: a single {@link
 * ScanTaskGroup} produced by this scan contains <em>only</em> {@link FileScanTask}s or
 * <em>only</em> {@link BqStreamScanTask}s, never both. The reader factory at execution time relies
 * on this invariant to route each partition to the correct read path.
 *
 * <p>Unlike {@link SparkPartitioningAwareScan}, this scan does not implement {@code
 * SupportsReportPartitioning} because the BigQuery Storage Read API stream half has no Iceberg
 * partition spec — there is no shared grouping key across the two halves.
 */
class HybridSparkScan extends SparkScan {

  private final SparkSession spark;
  private final Supplier<FileIO> fileIO;
  private final SparkReadConf readConf;
  private final Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> fileScan;
  private final BqAdvancedScanPlanner bqPlanner;
  private final HybridReaderConfig readerConfig;

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
    return new HybridSparkBatch(
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
