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

/**
 * A {@link SparkBatch} variant for {@link HybridSparkScan} that routes each input partition to the
 * correct reader via {@link HybridColumnarReaderFactory}.
 *
 * <p>The base {@link SparkBatch#createReaderFactory()} chooses one factory for the entire batch
 * based on whether <em>all</em> task groups are FileScanTasks of a vectorizable format. With a
 * heterogeneous task list it falls through to the row-based reader, which is the wrong choice for
 * the file half. This override keeps the file-half columnar by hard-coding Parquet vectorization
 * (the BigLake/Managed-Iceberg primary case) and wraps the result in {@link
 * HybridColumnarReaderFactory} so per-partition routing kicks in at execution time.
 *
 * <p>Refining file-format detection (Parquet vs ORC vs row-based fallback for the file half) is
 * deferred to Slice 4; today, scanning ORC or delete-laden file partitions through this batch will
 * not fall back to row reads automatically.
 */
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
        sparkContext,
        table,
        fileIO,
        readConf,
        groupingKeyType,
        taskGroups,
        expectedSchema,
        scanHashCode);
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
