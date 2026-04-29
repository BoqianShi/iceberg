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

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.BqStreamScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;

/**
 * Driver-side planner for the BigQuery Storage Read API stream half of a HybridStream scan.
 *
 * <p>Implementations call the BigQuery {@code GenerateScanPlan} RPC (see {@code
 * com.google.cloud.bigquery.storage.v1beta2.BigQueryRead}) and translate the response into a list
 * of {@link BqStreamScanTask}s. Together with the file-task half produced by Iceberg's standard
 * {@code Scan.planFiles()}, these tasks form the heterogeneous task list consumed by {@link
 * HybridSparkScan}.
 *
 * <p>This SPI exists so the {@code iceberg-spark-3.5_2.12} module does not have a hard dependency
 * on the BigQuery Storage Read API libraries; the real implementation lives in a downstream module
 * and is supplied at scan-build time. {@link #noop()} returns an implementation that produces no
 * stream tasks, which is the correct behavior for non-{@code bq-advanced} tables and for unit tests
 * of the file-only path.
 */
public interface BqAdvancedScanPlanner {
  /**
   * Plan the BigQuery Storage Read API streams that contribute to this scan.
   *
   * @param table the Iceberg table being scanned
   * @param projection the projected schema after column pruning
   * @param filters scan filter expressions to push down to {@code GenerateScanPlan}
   * @return the list of stream tasks; never null, may be empty
   */
  List<BqStreamScanTask> planStreams(Table table, Schema projection, List<Expression> filters);

  /** Returns a planner that produces no stream tasks. */
  static BqAdvancedScanPlanner noop() {
    return (table, projection, filters) -> Collections.emptyList();
  }
}
