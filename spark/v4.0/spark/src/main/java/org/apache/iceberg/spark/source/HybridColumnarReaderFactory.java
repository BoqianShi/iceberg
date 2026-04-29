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

import org.apache.iceberg.BqStreamScanTask;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A {@link PartitionReaderFactory} that routes each Spark input partition to either Iceberg's
 * standard file-based reader (delegated to a wrapped factory) or a BigQuery Storage Read API stream
 * reader, based on the homogeneous task type within the partition.
 *
 * <p>{@link HybridSparkScan} guarantees that every partition is homogeneous (all tasks are {@link
 * org.apache.iceberg.FileScanTask}s, or all are {@link BqStreamScanTask}s). This factory relies on
 * that invariant and delegates per-partition dispatch via {@link
 * SparkInputPartition#allTasksOfType(Class)}.
 *
 * <p>BigQuery Storage stream reads are always columnar (Arrow record batches → Spark {@link
 * ColumnarBatch}). The wrapped delegate decides whether file partitions can be read columnar
 * (Parquet/ORC vectorized) or must fall back to row-based reads.
 */
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
      return true;
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
