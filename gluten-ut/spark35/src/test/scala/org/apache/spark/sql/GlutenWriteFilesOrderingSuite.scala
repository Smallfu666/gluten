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
package org.apache.spark.sql

class GlutenWriteFilesOrderingSuite extends GlutenSQLTestsBaseTrait {

  test("GLUTEN-9789: WriteFiles ordering broken by SMJ-to-SHJ conversion") {
    withSQLConf("spark.sql.optimizer.plannedWrite.enabled" -> "true") {
      withTable("t1_9789", "t2_9789") {
        withTempPath { path =>
          val df1 = spark.range(0, 1000).selectExpr("id as id1", "id % 10 as p", "cast(id as string) as v1")
          val df2 = spark.range(0, 1000).selectExpr("id as id2", "id % 10 as p", "cast(id as string) as v2")
          df1.write.saveAsTable("t1_9789")
          df2.write.saveAsTable("t2_9789")

          val joined = spark.table("t1_9789").join(spark.table("t2_9789"), "p")
          joined.write.partitionBy("p").format("parquet").mode("overwrite").save(path.getCanonicalPath)

          // Verify row count correctness
          val readBack = spark.read.parquet(path.getCanonicalPath)
          assert(readBack.count() == 100000L)
        }
      }
    }
  }
}
