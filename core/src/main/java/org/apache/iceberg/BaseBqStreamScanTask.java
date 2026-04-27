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

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class BaseBqStreamScanTask implements BqStreamScanTask {
  private final String streamName;
  private final String readSessionName;
  private final byte[] serializedArrowSchema;
  private final String[] selectedFields;
  private final long estimatedRowCount;
  private final long estimatedSizeBytes;
  private transient volatile List<String> selectedFieldsList = null;

  public BaseBqStreamScanTask(
      String streamName,
      String readSessionName,
      byte[] serializedArrowSchema,
      List<String> selectedFields,
      long estimatedRowCount,
      long estimatedSizeBytes) {
    Preconditions.checkArgument(streamName != null, "Invalid stream name: null");
    Preconditions.checkArgument(readSessionName != null, "Invalid read session name: null");
    Preconditions.checkArgument(
        serializedArrowSchema != null, "Invalid serialized Arrow schema: null");
    Preconditions.checkArgument(selectedFields != null, "Invalid selected fields: null");
    this.streamName = streamName;
    this.readSessionName = readSessionName;
    this.serializedArrowSchema = Arrays.copyOf(serializedArrowSchema, serializedArrowSchema.length);
    this.selectedFields = selectedFields.toArray(new String[0]);
    this.estimatedRowCount = estimatedRowCount;
    this.estimatedSizeBytes = estimatedSizeBytes;
  }

  @Override
  public String streamName() {
    return streamName;
  }

  @Override
  public String readSessionName() {
    return readSessionName;
  }

  @Override
  public byte[] serializedArrowSchema() {
    return Arrays.copyOf(serializedArrowSchema, serializedArrowSchema.length);
  }

  @Override
  public List<String> selectedFields() {
    if (selectedFieldsList == null) {
      this.selectedFieldsList = ImmutableList.copyOf(selectedFields);
    }

    return selectedFieldsList;
  }

  @Override
  public long estimatedRowsCount() {
    return estimatedRowCount;
  }

  @Override
  public long sizeBytes() {
    return estimatedSizeBytes;
  }
}
