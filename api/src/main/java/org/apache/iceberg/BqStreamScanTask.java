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

import java.util.List;

/**
 * A scan task over a single BigQuery Storage Read API stream.
 *
 * <p>Unlike {@link FileScanTask}, which references a byte range in a file accessed through {@link
 * org.apache.iceberg.io.FileIO}, a {@code BqStreamScanTask} references a server-side gRPC stream
 * produced by the BigQuery {@code GenerateScanPlan} API. The stream returns pre-parsed Arrow record
 * batches; readers consuming this task must therefore bypass {@link org.apache.iceberg.io.FileIO}
 * entirely and connect directly to the BigQuery Storage Read API.
 *
 * <p>A heterogeneous {@link Scan} may produce both {@code FileScanTask} and {@code
 * BqStreamScanTask} instances within a single scan; engines that bin-pack tasks into partitions
 * must keep the two task types in separate partitions so that a single {@link
 * org.apache.spark.sql.connector.read.PartitionReader} (or equivalent) can handle a partition with
 * a single read strategy.
 */
public interface BqStreamScanTask extends ScanTask {
  /**
   * The fully-qualified name of the BigQuery Storage Read API stream this task reads.
   *
   * @return the stream resource name (e.g. {@code projects/.../sessions/.../streams/0})
   */
  String streamName();

  /** The fully-qualified name of the {@code ReadSession} this stream belongs to. */
  String readSessionName();

  /**
   * The serialized Arrow IPC schema describing the rows produced by this stream.
   *
   * <p>The bytes match the value of {@code ReadSession.ArrowSchema.serialized_schema} returned from
   * the BigQuery Storage Read API.
   *
   * @return a defensive copy of the serialized Arrow schema
   */
  byte[] serializedArrowSchema();

  /** The list of fields projected from the source table, in stream order. */
  List<String> selectedFields();

  @Override
  default boolean isBqStreamScanTask() {
    return true;
  }

  @Override
  default BqStreamScanTask asBqStreamScanTask() {
    return this;
  }
}
