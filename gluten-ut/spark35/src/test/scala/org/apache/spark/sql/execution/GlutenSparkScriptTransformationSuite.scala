/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution

import org.apache.spark.sql.GlutenSQLTestsBaseTrait

class GlutenSparkScriptTransformationSuite
  extends SparkScriptTransformationSuite
  with GlutenSQLTestsBaseTrait {

  test("GLUTEN-12649: Velox MemoryManager teardown flake with script transformation") {
    for (_ <- 1 to 20) {
      val df = spark.range(1000).selectExpr("CAST(id AS STRING) as a")
      df.createOrReplaceTempView("t_12649")
      spark.sql("SELECT TRANSFORM(a) USING 'cat' AS (a) FROM t_12649 LIMIT 10").collect()
    }
  }

  test("GLUTEN-12649: Missing command teardown path") {
    for (_ <- 1 to 20) {
      val df = spark.range(1000).selectExpr("CAST(id AS STRING) as a")
      df.createOrReplaceTempView("t_12649_err")
      intercept[Exception] {
        spark.sql("SELECT TRANSFORM(a) USING 'some_non_existent_command' AS (a) FROM t_12649_err").collect()
      }
    }
  }
}
