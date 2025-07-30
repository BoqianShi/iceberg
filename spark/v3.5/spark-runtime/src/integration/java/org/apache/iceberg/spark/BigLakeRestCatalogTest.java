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
package org.apache.iceberg.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BigLakeRestCatalogTest {

  private static SparkSession spark;

  private static final String WAREHOUSE_BUCKET = "gs://boqianshi-iceberg-vended-test";
  private static final String PROJECT_ID = "google.com:hadoop-cloud-dev";

  @BeforeClass
  public static void setupSpark() {
    spark =
            SparkSession.builder()
                    .master("local[2]")
                    .config("spark.sql.defaultCatalog", "biglake")
                    .config("spark.sql.catalog.biglake", "org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog.biglake.type", "rest")
                    .config(
                            "spark.sql.catalog.biglake.uri",
                            "https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
                    .config("spark.sql.catalog.biglake.warehouse", WAREHOUSE_BUCKET)
                    .config("spark.sql.catalog.biglake.header.x-goog-user-project", PROJECT_ID)
                    .config(
                            "spark.sql.catalog.biglake.rest.auth.type",
                            "org.apache.iceberg.gcp.auth.GoogleAuthManager")
                    .config(
                            "spark.sql.extensions",
                            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                    .config("spark.sql.catalog.biglake.rest-metrics-reporting-enabled", "false")
                    .config(
                            "spark.sql.catalog.biglake.header.X-Iceberg-Access-Delegation",
                            "vended-credentials")
                    .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testSparkSession() {
    // Simple test to ensure the Spark session is created successfully
    spark.sql("SELECT 1").show();
  }

  @Test
  public void testCreateTableAndSelect() {
    spark.sql("USE biglake.test");
    spark.sql("SELECT * from test.shakespeare where word='spark'").show();
  }
}
