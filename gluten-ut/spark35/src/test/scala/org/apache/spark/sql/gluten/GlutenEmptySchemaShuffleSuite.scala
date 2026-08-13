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
package org.apache.spark.sql.gluten

import org.apache.gluten.execution.ColumnarCollectLimitExec
import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.execution.ColumnarShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col

class GlutenEmptySchemaShuffleSuite extends GlutenSQLTestsTrait with AdaptiveSparkPlanHelper {

  testGluten("SPARK-23627 style: empty schema shuffle preserves row count") {
    withTempDir {
      dir =>
        val df = spark.range(0, 10).select()
        assert(df.schema.fields.isEmpty, "expected 0-column schema")
        val repartitioned = df.repartition(2)
        val result = repartitioned.collect()

        assert(
          result.length == 10,
          s"expected 10 rows, got ${result.length} (row count lost in shuffle)")

        val plan = repartitioned.queryExecution.executedPlan
        val columnarShuffles = collect(plan) {
          case p: ColumnarShuffleExchangeExec => p
        }
        assert(columnarShuffles.nonEmpty, "expected native columnar shuffle, got row-mode fallback")
    }
  }

  testGluten("empty schema shuffle: native columnar path is taken") {
    val df = spark.range(0, 10).select()
    val repartitioned = df.repartition(2)
    val result = repartitioned.collect()

    val plan = repartitioned.queryExecution.executedPlan
    val columnarShuffles = collect(plan) {
      case p: ColumnarShuffleExchangeExec => p
    }

    info(s"executedPlan: ${plan.toString.take(500)}")
    info(s"columnarShuffleCount=${columnarShuffles.size}")

    assert(
      columnarShuffles.nonEmpty,
      "expected ColumnarShuffleExchangeExec (native), got row-mode fallback")
    assert(result.length == 10, "row count not preserved")
  }

  testGluten("empty schema shuffle: 1-column regression") {
    val df = spark.range(0, 10).select(col("id"))
    val result = df.repartition(2).collect()
    assert(result.length == 10)
    val ids = result.map(_.getLong(0)).sorted.toSeq
    assert(ids == (0L until 10L).toSeq, s"expected 0..9, got $ids")
  }

  testGluten("empty schema shuffle: 0-column 0-row") {
    val df = spark.range(0, 0).select()
    val result = df.repartition(2).collect()
    assert(result.length == 0)
  }

  testGluten("empty schema shuffle: ConstantFolding disabled") {
    withSQLConf(
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      val df = spark.range(0, 10).select()
      val result = df.repartition(2).collect()
      assert(result.length == 10)
    }
  }

  testGluten("empty schema: columnar CollectLimit on 0-column DataFrame") {
    val df = spark.range(0, 10).select()
    val limited = df.limit(5)
    val result = limited.collect()

    val plan = limited.queryExecution.executedPlan
    val columnarLimit = collect(plan) {
      case p: ColumnarCollectLimitExec => p
    }
    info(s"executedPlan: ${plan.toString.take(500)}")
    info(s"columnarLimitCount=${columnarLimit.size}")

    assert(result.length == 5, s"expected 5 rows, got ${result.length}")
    assert(
      columnarLimit.nonEmpty,
      "expected ColumnarCollectLimitExec (native), got row-mode CollectLimitExec")
  }

  testGluten("empty schema shuffle: sort threshold does not select sort writer") {
    withSQLConf(
      "spark.gluten.sql.columnar.shuffle.sort.partitions.threshold" -> "1") {
      val df = spark.range(0, 10).select()
      val repartitioned = df.repartition(2)
      val result = repartitioned.collect()

      val plan = repartitioned.queryExecution.executedPlan
      val columnarShuffles = collect(plan) {
        case p: ColumnarShuffleExchangeExec => p
      }
      info(s"executedPlan: ${plan.toString.take(500)}")
      columnarShuffles.foreach { cs =>
        info(s"shuffle_writer_type=${cs.shuffleWriterType.name}")
      }

      assert(columnarShuffles.nonEmpty, "expected native columnar shuffle")
      assert(
        columnarShuffles.forall(_.shuffleWriterType.name == "hash"),
        "expected hash writer for 0-column schema even with sort threshold=1, got: " +
          columnarShuffles.map(_.shuffleWriterType.name).mkString(","))
      assert(result.length == 10, s"row count lost: got ${result.length}")
    }
  }

}
