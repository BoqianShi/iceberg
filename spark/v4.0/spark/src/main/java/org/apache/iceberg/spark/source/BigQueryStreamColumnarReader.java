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

/**
 * A {@link PartitionReader} for Spark partitions whose tasks are exclusively {@link
 * BqStreamScanTask}s. Bypasses Iceberg's {@code FileIO}/Parquet path entirely; the bytes flow as
 * Arrow record batches over a BigQuery Storage Read API gRPC stream and are converted to Spark
 * {@link ColumnarBatch} by the spark-bigquery-connector.
 *
 * <p>This class is a thin adapter: it constructs the connector's {@link ArrowInputPartitionContext}
 * lazily on the first call to {@link #next()} (so that {@code TaskContext.get()} is set, which the
 * connector's metrics registration requires) and delegates {@code next}/{@code get}/{@code close}
 * to the resulting {@link InputPartitionReaderContext}.
 *
 * <p>All tasks within a single partition are assumed to belong to the same {@code ReadSession} and
 * therefore share the same Arrow schema and projected fields. {@link HybridSparkScan} guarantees
 * homogeneous bin-packing of tasks; the additional same-{@code ReadSession} assumption is
 * documented for the planner that supplies the tasks.
 */
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
  public boolean next() throws IOException {
    return delegate().next();
  }

  @Override
  public ColumnarBatch get() {
    return delegate().get();
  }

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
        new ReadSessionResponse(synthesizeReadSession(first), null);

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
