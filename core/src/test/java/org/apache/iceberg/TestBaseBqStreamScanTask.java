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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestBaseBqStreamScanTask {

  private static final String STREAM_NAME =
      "projects/test-project/locations/us/sessions/abc/streams/0";
  private static final String SESSION_NAME = "projects/test-project/locations/us/sessions/abc";
  private static final byte[] ARROW_SCHEMA = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
  private static final List<String> SELECTED_FIELDS = ImmutableList.of("id", "name", "ts");
  private static final long ROW_COUNT = 12345L;
  private static final long SIZE_BYTES = 6_543_210L;

  private static BaseBqStreamScanTask newTask() {
    return new BaseBqStreamScanTask(
        STREAM_NAME, SESSION_NAME, ARROW_SCHEMA, SELECTED_FIELDS, ROW_COUNT, SIZE_BYTES);
  }

  @Test
  public void accessorsReturnConstructorValues() {
    BaseBqStreamScanTask task = newTask();

    assertThat(task.streamName()).isEqualTo(STREAM_NAME);
    assertThat(task.readSessionName()).isEqualTo(SESSION_NAME);
    assertThat(task.serializedArrowSchema()).containsExactly(ARROW_SCHEMA);
    assertThat(task.selectedFields()).isEqualTo(SELECTED_FIELDS);
    assertThat(task.estimatedRowsCount()).isEqualTo(ROW_COUNT);
    assertThat(task.sizeBytes()).isEqualTo(SIZE_BYTES);
    assertThat(task.filesCount()).isEqualTo(1);
  }

  @Test
  public void discriminationReportsBqStreamScanTask() {
    ScanTask task = newTask();

    assertThat(task.isBqStreamScanTask()).isTrue();
    assertThat(task.asBqStreamScanTask()).isSameAs(task);
    assertThat(task.isFileScanTask()).isFalse();
    assertThat(task.isDataTask()).isFalse();
  }

  @Test
  public void serializedArrowSchemaIsDefensivelyCopied() {
    byte[] mutable = Arrays.copyOf(ARROW_SCHEMA, ARROW_SCHEMA.length);
    BaseBqStreamScanTask task =
        new BaseBqStreamScanTask(
            STREAM_NAME, SESSION_NAME, mutable, SELECTED_FIELDS, ROW_COUNT, SIZE_BYTES);

    mutable[0] = 99;
    assertThat(task.serializedArrowSchema()[0]).isEqualTo(ARROW_SCHEMA[0]);

    byte[] returned = task.serializedArrowSchema();
    returned[0] = 99;
    assertThat(task.serializedArrowSchema()[0]).isEqualTo(ARROW_SCHEMA[0]);
  }

  @Test
  public void rejectsNullArguments() {
    assertThatThrownBy(
            () ->
                new BaseBqStreamScanTask(
                    null, SESSION_NAME, ARROW_SCHEMA, SELECTED_FIELDS, ROW_COUNT, SIZE_BYTES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stream name: null");

    assertThatThrownBy(
            () ->
                new BaseBqStreamScanTask(
                    STREAM_NAME, null, ARROW_SCHEMA, SELECTED_FIELDS, ROW_COUNT, SIZE_BYTES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid read session name: null");

    assertThatThrownBy(
            () ->
                new BaseBqStreamScanTask(
                    STREAM_NAME, SESSION_NAME, null, SELECTED_FIELDS, ROW_COUNT, SIZE_BYTES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid serialized Arrow schema: null");

    assertThatThrownBy(
            () ->
                new BaseBqStreamScanTask(
                    STREAM_NAME, SESSION_NAME, ARROW_SCHEMA, null, ROW_COUNT, SIZE_BYTES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid selected fields: null");
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void roundTripPreservesAllFields(TestHelpers.RoundTripSerializer<BaseBqStreamScanTask> ser)
      throws Exception {
    BaseBqStreamScanTask original = newTask();
    BaseBqStreamScanTask roundTripped = ser.apply(original);

    assertThat(roundTripped.streamName()).isEqualTo(STREAM_NAME);
    assertThat(roundTripped.readSessionName()).isEqualTo(SESSION_NAME);
    assertThat(roundTripped.serializedArrowSchema()).containsExactly(ARROW_SCHEMA);
    assertThat(roundTripped.selectedFields()).isEqualTo(SELECTED_FIELDS);
    assertThat(roundTripped.estimatedRowsCount()).isEqualTo(ROW_COUNT);
    assertThat(roundTripped.sizeBytes()).isEqualTo(SIZE_BYTES);
    assertThat(roundTripped.isBqStreamScanTask()).isTrue();
  }
}
