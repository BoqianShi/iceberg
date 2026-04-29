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

import org.apache.iceberg.Table;

/**
 * Detection helpers for the {@code bq-advanced} table mode that triggers HybridStream planning.
 *
 * <p>A BigQuery Managed Iceberg table is considered "bq-advanced" — meaning it has data resident in
 * BigQuery's Vortex/FGAC storage in addition to GCS-backed Parquet — when the table property
 * {@value #ENABLE_BIGQUERY_ADVANCED} is set to {@code true}. {@link SparkScanBuilder} uses this
 * signal to route reads through {@link HybridSparkScan} instead of the standard {@link
 * SparkBatchQueryScan}.
 *
 * <p>This property-based detection is the simplest correct signal available today and is good
 * enough for the Slice 4 scaffold. If/when BigLake metadata exposes the same signal more reliably
 * (e.g. through catalog hints or table metadata APIs), prefer that source and treat the property as
 * a fallback.
 */
final class BqAdvancedTableProperty {

  /** Iceberg table property: when set to {@code "true"}, route reads through HybridStream. */
  static final String ENABLE_BIGQUERY_ADVANCED = "enable_bigquery_advanced";

  private BqAdvancedTableProperty() {}

  /**
   * @return {@code true} if the table has property {@value #ENABLE_BIGQUERY_ADVANCED} set to a
   *     truthy value, {@code false} otherwise (including missing / unparseable values).
   */
  static boolean isEnabled(Table table) {
    if (table == null || table.properties() == null) {
      return false;
    }
    String value = table.properties().get(ENABLE_BIGQUERY_ADVANCED);
    return value != null && Boolean.parseBoolean(value);
  }
}
