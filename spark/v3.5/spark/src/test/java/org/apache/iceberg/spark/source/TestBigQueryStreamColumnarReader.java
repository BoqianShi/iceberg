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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spark.bigquery.v2.context.InputPartitionReaderContext;
import org.apache.iceberg.BqStreamScanTask;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

public class TestBigQueryStreamColumnarReader {

  @Test
  @SuppressWarnings("unchecked")
  public void delegatesNextGetClose() throws Exception {
    InputPartitionReaderContext<ColumnarBatch> ctx = mock(InputPartitionReaderContext.class);
    ColumnarBatch batch = mock(ColumnarBatch.class);
    when(ctx.next()).thenReturn(true, false);
    when(ctx.get()).thenReturn(batch);

    BigQueryStreamColumnarReader reader = new BigQueryStreamColumnarReader(ctx);

    assertThat(reader.next()).isTrue();
    assertThat(reader.get()).isSameAs(batch);
    assertThat(reader.next()).isFalse();
    reader.close();

    verify(ctx).close();
  }

  @Test
  public void closeIsNoOpWhenContextNeverOpened() throws Exception {
    SparkInputPartition partition = mock(SparkInputPartition.class);
    when(partition.allTasksOfType(BqStreamScanTask.class)).thenReturn(true);
    HybridReaderConfig config = new HybridReaderConfig(null, null, null, null, null);

    BigQueryStreamColumnarReader reader = new BigQueryStreamColumnarReader(partition, config);
    reader.close();
    // Did not invoke partition.taskGroup() because the lazy delegate was never opened.
    verify(partition, org.mockito.Mockito.never()).taskGroup();
  }

  @Test
  public void rejectsNonStreamPartition() {
    SparkInputPartition partition = mock(SparkInputPartition.class);
    when(partition.allTasksOfType(BqStreamScanTask.class)).thenReturn(false);
    HybridReaderConfig config = new HybridReaderConfig(null, null, null, null, null);

    assertThatThrownBy(() -> new BigQueryStreamColumnarReader(partition, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("All tasks in the partition must be BqStreamScanTask");
  }

  @Test
  public void rejectsNullArguments() {
    HybridReaderConfig config = new HybridReaderConfig(null, null, null, null, null);
    assertThatThrownBy(() -> new BigQueryStreamColumnarReader(null, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid partition: null");

    SparkInputPartition partition = mock(SparkInputPartition.class);
    when(partition.allTasksOfType(BqStreamScanTask.class)).thenReturn(true);
    assertThatThrownBy(() -> new BigQueryStreamColumnarReader(partition, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid HybridReaderConfig: null");
  }
}
