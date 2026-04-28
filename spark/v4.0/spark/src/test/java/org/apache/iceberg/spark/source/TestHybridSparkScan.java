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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.BaseBqStreamScanTask;
import org.apache.iceberg.BqStreamScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestHybridSparkScan {

  private static final long SPLIT_SIZE = 128L * 1024 * 1024; // 128 MB
  private static final int SPLIT_LOOKBACK = 10;
  private static final long OPEN_FILE_COST = 4L * 1024 * 1024; // 4 MB

  @Test
  public void emptyInputsProduceEmptyOutput() {
    List<ScanTaskGroup<ScanTask>> groups =
        HybridSparkScan.planHomogeneousTaskGroups(
            Collections.emptyList(),
            Collections.emptyList(),
            SPLIT_SIZE,
            SPLIT_LOOKBACK,
            OPEN_FILE_COST);

    assertThat(groups).isEmpty();
  }

  @Test
  public void onlyFileTasksProduceFileOnlyGroups() {
    List<FileScanTask> files =
        ImmutableList.of(
            mockFileScanTask(50L * 1024 * 1024),
            mockFileScanTask(50L * 1024 * 1024),
            mockFileScanTask(50L * 1024 * 1024));

    List<ScanTaskGroup<ScanTask>> groups =
        HybridSparkScan.planHomogeneousTaskGroups(
            files, Collections.emptyList(), SPLIT_SIZE, SPLIT_LOOKBACK, OPEN_FILE_COST);

    assertThat(groups).isNotEmpty();
    assertAllGroupsHomogeneous(groups);
    assertThat(totalTaskCount(groups)).isEqualTo(files.size());
    assertThat(taskTypesAcrossGroups(groups)).containsExactly(FileScanTask.class);
  }

  @Test
  public void onlyStreamTasksProduceStreamOnlyGroups() {
    List<BqStreamScanTask> streams =
        ImmutableList.of(
            newStreamTask("streams/0", 10_000_000L),
            newStreamTask("streams/1", 10_000_000L),
            newStreamTask("streams/2", 10_000_000L));

    List<ScanTaskGroup<ScanTask>> groups =
        HybridSparkScan.planHomogeneousTaskGroups(
            Collections.emptyList(), streams, SPLIT_SIZE, SPLIT_LOOKBACK, OPEN_FILE_COST);

    assertThat(groups).isNotEmpty();
    assertAllGroupsHomogeneous(groups);
    assertThat(totalTaskCount(groups)).isEqualTo(streams.size());
    assertThat(taskTypesAcrossGroups(groups)).containsExactly(BqStreamScanTask.class);
  }

  @Test
  public void mixedTasksProduceHomogeneousGroups() {
    List<FileScanTask> files =
        ImmutableList.of(
            mockFileScanTask(50L * 1024 * 1024),
            mockFileScanTask(50L * 1024 * 1024),
            mockFileScanTask(50L * 1024 * 1024));
    List<BqStreamScanTask> streams =
        ImmutableList.of(
            newStreamTask("streams/0", 10_000_000L), newStreamTask("streams/1", 10_000_000L));

    List<ScanTaskGroup<ScanTask>> groups =
        HybridSparkScan.planHomogeneousTaskGroups(
            files, streams, SPLIT_SIZE, SPLIT_LOOKBACK, OPEN_FILE_COST);

    assertAllGroupsHomogeneous(groups);
    assertThat(totalTaskCount(groups)).isEqualTo(files.size() + streams.size());
    assertThat(taskTypesAcrossGroups(groups))
        .containsExactlyInAnyOrder(FileScanTask.class, BqStreamScanTask.class);
  }

  @Test
  public void streamTasksAreNotSplitOrMerged() {
    BqStreamScanTask huge = newStreamTask("streams/big", SPLIT_SIZE * 8);
    BqStreamScanTask small1 = newStreamTask("streams/small1", 1L);
    BqStreamScanTask small2 = newStreamTask("streams/small2", 1L);

    List<ScanTaskGroup<ScanTask>> groups =
        HybridSparkScan.planHomogeneousTaskGroups(
            Collections.emptyList(),
            ImmutableList.of(huge, small1, small2),
            SPLIT_SIZE,
            SPLIT_LOOKBACK,
            OPEN_FILE_COST);

    assertThat(totalTaskCount(groups)).isEqualTo(3);
    for (ScanTaskGroup<ScanTask> group : groups) {
      for (ScanTask task : group.tasks()) {
        assertThat(task).isInstanceOf(BqStreamScanTask.class);
        BqStreamScanTask stream = (BqStreamScanTask) task;
        assertThat(stream.streamName()).isIn("streams/big", "streams/small1", "streams/small2");
      }
    }
  }

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

  private static BqStreamScanTask newStreamTask(String streamName, long sizeBytes) {
    return new BaseBqStreamScanTask(
        streamName,
        "projects/test/locations/us/sessions/abc",
        new byte[] {0},
        ImmutableList.of("c1"),
        sizeBytes / 8,
        sizeBytes);
  }

  private static void assertAllGroupsHomogeneous(List<ScanTaskGroup<ScanTask>> groups) {
    for (ScanTaskGroup<ScanTask> group : groups) {
      List<ScanTask> tasks = ImmutableList.copyOf(group.tasks());
      assertThat(tasks).as("group must not be empty").isNotEmpty();
      Class<?> first = taskKind(tasks.get(0));
      for (ScanTask task : tasks) {
        assertThat(taskKind(task))
            .as("group with %s must not contain a %s", first, taskKind(task))
            .isEqualTo(first);
      }
    }
  }

  private static int totalTaskCount(List<ScanTaskGroup<ScanTask>> groups) {
    int total = 0;
    for (ScanTaskGroup<ScanTask> group : groups) {
      total += ImmutableList.copyOf(group.tasks()).size();
    }
    return total;
  }

  private static List<Class<?>> taskTypesAcrossGroups(List<ScanTaskGroup<ScanTask>> groups) {
    List<Class<?>> kinds = new java.util.ArrayList<>();
    for (ScanTaskGroup<ScanTask> group : groups) {
      ScanTask first = ImmutableList.copyOf(group.tasks()).get(0);
      Class<?> kind = taskKind(first);
      if (!kinds.contains(kind)) {
        kinds.add(kind);
      }
    }
    return kinds;
  }

  private static Class<?> taskKind(ScanTask task) {
    if (task instanceof FileScanTask) {
      return FileScanTask.class;
    }
    if (task instanceof BqStreamScanTask) {
      return BqStreamScanTask.class;
    }
    return task.getClass();
  }
}
