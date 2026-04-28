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

import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.cloud.spark.bigquery.metrics.SparkBigQueryReadSessionMetrics;
import java.io.Serializable;

/**
 * Executor-side configuration for {@link HybridColumnarReaderFactory}: the spark-bigquery-connector
 * factories needed to open a BigQuery Storage Read API gRPC stream.
 *
 * <p>Instances are constructed on the driver and broadcast to executors as part of the serialized
 * {@link HybridColumnarReaderFactory}. All component types are {@link Serializable} on the
 * connector side.
 *
 * <p>Credential vending into {@link BigQueryClientFactory} is intentionally out of scope for Slice
 * 3 of the HybridStream integration; the {@link BigQueryClientFactory} passed in is expected to
 * carry credentials by the time the {@link HybridSparkScan} is built. Slice 4 will resolve the
 * exact vending mechanism (single supplier vs per-stream tokens from {@code GenerateScanPlan}).
 */
public final class HybridReaderConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private final BigQueryClientFactory clientFactory;
  private final BigQueryTracerFactory tracerFactory;
  private final ReadRowsHelper.Options readOptions;
  private final ResponseCompressionCodec compressionCodec;
  private final SparkBigQueryReadSessionMetrics sessionMetrics;

  public HybridReaderConfig(
      BigQueryClientFactory clientFactory,
      BigQueryTracerFactory tracerFactory,
      ReadRowsHelper.Options readOptions,
      ResponseCompressionCodec compressionCodec,
      SparkBigQueryReadSessionMetrics sessionMetrics) {
    this.clientFactory = clientFactory;
    this.tracerFactory = tracerFactory;
    this.readOptions = readOptions;
    this.compressionCodec = compressionCodec;
    this.sessionMetrics = sessionMetrics;
  }

  public BigQueryClientFactory clientFactory() {
    return clientFactory;
  }

  public BigQueryTracerFactory tracerFactory() {
    return tracerFactory;
  }

  public ReadRowsHelper.Options readOptions() {
    return readOptions;
  }

  public ResponseCompressionCodec compressionCodec() {
    return compressionCodec;
  }

  public SparkBigQueryReadSessionMetrics sessionMetrics() {
    return sessionMetrics;
  }
}
