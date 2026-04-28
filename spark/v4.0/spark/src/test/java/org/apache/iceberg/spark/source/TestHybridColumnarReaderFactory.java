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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.BqStreamScanTask;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

public class TestHybridColumnarReaderFactory {

  private final HybridReaderConfig readerConfig =
      new HybridReaderConfig(null, null, null, null, null);

  @Test
  public void streamPartitionRoutesToBigQueryReader() {
    PartitionReaderFactory delegate = mock(PartitionReaderFactory.class);
    HybridColumnarReaderFactory factory = new HybridColumnarReaderFactory(delegate, readerConfig);

    SparkInputPartition partition = streamPartition();

    assertThat(factory.supportColumnarReads(partition)).isTrue();
    verify(delegate, never()).supportColumnarReads(partition);
  }

  @Test
  public void filePartitionDelegatesSupportColumnarReads() {
    PartitionReaderFactory delegate = mock(PartitionReaderFactory.class);
    when(delegate.supportColumnarReads(org.mockito.ArgumentMatchers.any(InputPartition.class)))
        .thenReturn(true);
    HybridColumnarReaderFactory factory = new HybridColumnarReaderFactory(delegate, readerConfig);

    SparkInputPartition partition = filePartition();

    assertThat(factory.supportColumnarReads(partition)).isTrue();
    verify(delegate).supportColumnarReads(partition);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void filePartitionDelegatesColumnarReader() {
    PartitionReaderFactory delegate = mock(PartitionReaderFactory.class);
    PartitionReader<ColumnarBatch> expected = mock(PartitionReader.class);
    when(delegate.createColumnarReader(org.mockito.ArgumentMatchers.any(InputPartition.class)))
        .thenReturn(expected);
    HybridColumnarReaderFactory factory = new HybridColumnarReaderFactory(delegate, readerConfig);

    SparkInputPartition partition = filePartition();

    assertThat(factory.createColumnarReader(partition)).isSameAs(expected);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void filePartitionDelegatesRowReader() {
    PartitionReaderFactory delegate = mock(PartitionReaderFactory.class);
    PartitionReader<InternalRow> expected = mock(PartitionReader.class);
    when(delegate.createReader(org.mockito.ArgumentMatchers.any(InputPartition.class)))
        .thenReturn(expected);
    HybridColumnarReaderFactory factory = new HybridColumnarReaderFactory(delegate, readerConfig);

    assertThat(factory.createReader(filePartition())).isSameAs(expected);
  }

  @Test
  public void streamPartitionRowReaderThrows() {
    PartitionReaderFactory delegate = mock(PartitionReaderFactory.class);
    HybridColumnarReaderFactory factory = new HybridColumnarReaderFactory(delegate, readerConfig);

    assertThatThrownBy(() -> factory.createReader(streamPartition()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("BqStreamScanTask partitions only support columnar reads");
  }

  @Test
  public void rejectsNullDelegate() {
    assertThatThrownBy(() -> new HybridColumnarReaderFactory(null, readerConfig))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file delegate factory: null");
  }

  @Test
  public void rejectsNullReaderConfig() {
    PartitionReaderFactory delegate = mock(PartitionReaderFactory.class);
    assertThatThrownBy(() -> new HybridColumnarReaderFactory(delegate, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid HybridReaderConfig: null");
  }

  private static SparkInputPartition streamPartition() {
    SparkInputPartition partition = mock(SparkInputPartition.class);
    when(partition.allTasksOfType(BqStreamScanTask.class)).thenReturn(true);
    return partition;
  }

  private static SparkInputPartition filePartition() {
    SparkInputPartition partition = mock(SparkInputPartition.class);
    when(partition.allTasksOfType(BqStreamScanTask.class)).thenReturn(false);
    return partition;
  }
}
