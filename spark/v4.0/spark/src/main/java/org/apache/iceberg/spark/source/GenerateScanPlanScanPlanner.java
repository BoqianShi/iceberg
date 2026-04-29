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
import java.util.List;
import org.apache.iceberg.BqStreamScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * {@link BqAdvancedScanPlanner} implementation that calls the BigQuery {@code GenerateScanPlan} RPC
 * and translates the response into {@link BqStreamScanTask}s.
 *
 * <p><strong>Slice 4 status:</strong> this planner is a scaffold. The actual RPC call is
 * unimplemented because the {@code v1beta2} {@code BigQueryRead.GenerateScanPlan} API jar is hosted
 * on Google's internal artifact registry and is not yet wired into the Iceberg build (see the
 * {@code TODO} in {@code spark/v3.5/build.gradle}). Once the dependency lands, fill in {@link
 * #planStreams(Table, java.util.List)} per the sketch below.
 */
class GenerateScanPlanScanPlanner implements BqAdvancedScanPlanner {

  private final BigQueryClientFactory clientFactory;

  GenerateScanPlanScanPlanner(BigQueryClientFactory clientFactory) {
    Preconditions.checkArgument(clientFactory != null, "Invalid BigQueryClientFactory: null");
    this.clientFactory = clientFactory;
  }

  /** Exposed for testing. */
  BigQueryClientFactory clientFactory() {
    return clientFactory;
  }

  @Override
  public List<BqStreamScanTask> planStreams(
      Table table, Schema projection, List<Expression> filters) {
    // TODO(slice-4-artifact-registry): wire this method to the v1beta2 BigQueryRead client.
    //
    //   The expected shape, modeled on the existing v1 createReadSession path in
    //   spark-bigquery-connector's ReadSessionCreator and on the GenerateScanPlan RPC the
    //   Dataproc team has flagged in com.google.cloud.bigquery.storage.v1beta2.BigQueryRead:
    //
    //     try (com.google.cloud.bigquery.storage.v1beta2.BigQueryReadClient client =
    //              clientFactory.getBigQueryReadClientV1Beta2()) {
    //       GenerateScanPlanRequest request = GenerateScanPlanRequest.newBuilder()
    //           .setParent("projects/" + clientFactory.getProjectId())
    //           .setTable(toBqTableRef(table))
    //           .addAllSelectedFields(projection.columns().stream()
    //               .map(Types.NestedField::name).collect(Collectors.toList()))
    //           .setRowRestriction(toBqExpression(filters))
    //           .build();
    //       GenerateScanPlanResponse response = client.generateScanPlan(request);
    //       List<BqStreamScanTask> tasks = Lists.newArrayList();
    //       String readSessionName = response.getReadSession().getName();
    //       byte[] arrowSchemaBytes = response.getReadSession().getArrowSchema()
    //                                        .getSerializedSchema().toByteArray();
    //       List<String> selectedFields = projection.columns().stream()
    //           .map(Types.NestedField::name).collect(Collectors.toList());
    //       for (ReadStream stream : response.getStreamsList()) {
    //         tasks.add(new BaseBqStreamScanTask(
    //             stream.getName(),
    //             readSessionName,
    //             arrowSchemaBytes,
    //             selectedFields,
    //             stream.getStats().getEstimatedRowCount(),
    //             stream.getStats().getEstimatedBytes()));
    //       }
    //       return tasks;
    //     }
    //
    //   Open questions for Slice 4 follow-up (tracked in slice-4-research-prompt.md):
    //     1. Coordinate of the v1beta2 jar on the internal artifact registry.
    //     2. Exact proto field names on GenerateScanPlanRequest / Response (the names above
    //        are best-guess from naming conventions in the v1 API).
    //     3. How to translate Iceberg table name -> BQ TableReference (project, dataset, table).
    //     4. How to translate Iceberg Expression filters -> BQ row_restriction string.
    //     5. Whether GenerateScanPlanResponse exposes a pre-allocated ReadSession proto, or
    //        we have to construct one from the response fields.
    throw new UnsupportedOperationException(
        "GenerateScanPlanScanPlanner.planStreams: unimplemented. The v1beta2 BigQueryRead "
            + "API jar is on Google's internal artifact registry and not yet wired into the "
            + "Iceberg build. Once the dep lands (see TODO in spark/v3.5/build.gradle), "
            + "implement per the inline sketch.");
  }
}
