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

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TestScanTaskDiscrimination {

  @Test
  public void defaultScanTaskIsNotBqStreamScanTask() {
    ScanTask task = new ScanTask() {};

    assertThat(task.isBqStreamScanTask()).isFalse();
    assertThat(task.isFileScanTask()).isFalse();
    assertThat(task.isDataTask()).isFalse();
  }

  @Test
  public void defaultAsBqStreamScanTaskThrows() {
    ScanTask task = new ScanTask() {};

    assertThatThrownBy(task::asBqStreamScanTask)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Not a BqStreamScanTask:");
  }

  @Test
  public void bqStreamScanTaskDiscriminatesAsItself() {
    BqStreamScanTask task = new StubBqStreamScanTask();

    assertThat(task.isBqStreamScanTask()).isTrue();
    assertThat(task.asBqStreamScanTask()).isSameAs(task);
    assertThat(task.isFileScanTask()).isFalse();
    assertThatThrownBy(task::asFileScanTask)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Not a FileScanTask:");
  }

  private static final class StubBqStreamScanTask implements BqStreamScanTask {
    @Override
    public String streamName() {
      return "projects/p/locations/l/sessions/s/streams/0";
    }

    @Override
    public String readSessionName() {
      return "projects/p/locations/l/sessions/s";
    }

    @Override
    public byte[] serializedArrowSchema() {
      return new byte[0];
    }

    @Override
    public List<String> selectedFields() {
      return Collections.emptyList();
    }
  }
}
