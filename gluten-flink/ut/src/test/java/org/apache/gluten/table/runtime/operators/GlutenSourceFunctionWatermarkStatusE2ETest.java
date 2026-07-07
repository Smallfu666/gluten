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
package org.apache.gluten.table.runtime.operators;

import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;

import io.github.zhztheplayer.velox4j.connector.KafkaConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.KafkaTableHandle;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanWithWatermarkNode;
import io.github.zhztheplayer.velox4j.plan.WatermarkPushDownSpec;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.VarCharType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test that verifies WatermarkStatus.IDLE is emitted through GlutenSourceFunction when
 * the native Kafka source detects idleness.
 *
 * <p>The test produces a few records to an embedded Kafka broker, starts a Flink MiniCluster job
 * with the Gluten native source pipeline, waits for the idle timeout to expire, and checks that
 * WatermarkStatus.IDLE is captured by a downstream operator.
 */
class GlutenSourceFunctionWatermarkStatusE2ETest {

  private static final int KAFKA_PORT = 19093;
  private static final long IDLE_TIMEOUT_MS = 5000;
  private static final long WATERMARK_INTERVAL_MS = 500;

  @RegisterExtension
  static final SharedKafkaTestResource KAFKA =
      new SharedKafkaTestResource()
          .withBrokerProperty("host.name", "127.0.0.1")
          .withBrokers(1)
          .registerListener(new PlainListener().onPorts(KAFKA_PORT));

  private static final CopyOnWriteArrayList<WatermarkStatus> capturedStatuses =
      new CopyOnWriteArrayList<>();

  @BeforeAll
  static void setupGluten() {
    Velox4jEnvironment.initializeOnce();
  }

  @BeforeEach
  void clearCaptured() {
    capturedStatuses.clear();
  }

  @AfterEach
  void ensureJobCancelled() {
    if (jobThread != null && jobThread.isAlive()) {
      jobThread.interrupt();
    }
  }

  private Thread jobThread;

  @Test
  void testIdleDetectionAfterStopWritingToKafka() throws Exception {
    String topic = "idle_e2e_" + UUID.randomUUID().toString().replace("-", "");
    KAFKA.getKafkaTestUtils().createTopic(topic, 1, (short) 1);
    KAFKA
        .getKafkaTestUtils()
        .produceRecords(
            List.of(
                jsonRecord(topic, "{\"id\":1000,\"name\":\"r0\"}"),
                jsonRecord(topic, "{\"id\":2000,\"name\":\"r1\"}"),
                jsonRecord(topic, "{\"id\":3000,\"name\":\"r2\"}")));

    // Build pipeline: GlutenStreamSource -> WatermarkStatusCaptureOperator
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
    env.getConfig().disableClosureCleaner();
    env.setParallelism(1);

    DataStreamSource<StatefulRecord> source = addSourceToEnv(env, topic);
    source
        .transform("capture", TypeInformation.of(StatefulRecord.class), new StatusCaptureOp())
        .setParallelism(1);

    // Run job in background thread
    jobThread =
        new Thread(
            () -> {
              try {
                env.execute("IdleDetectionE2ETest");
              } catch (Exception e) {
                // ignore cancellation
              }
            },
            "job-runner");
    jobThread.start();

    // Wait for initial records to be consumed
    Thread.sleep(WATERMARK_INTERVAL_MS * 4);

    // Wait for idle timeout
    Thread.sleep(IDLE_TIMEOUT_MS + 3000);

    // Cancel the job
    jobThread.interrupt();

    // Verify idle was detected
    assertThat(capturedStatuses)
        .as("Should have received WatermarkStatus.IDLE after idle timeout")
        .contains(WatermarkStatus.IDLE);
  }

  // -- helpers --

  private static DataStreamSource<StatefulRecord> addSourceToEnv(
      StreamExecutionEnvironment env, String topic) {
    RowType veloxRowType =
        new RowType(List.of("id", "name"), List.of(new BigIntType(), new VarCharType()));

    ProjectNode watermarkProject =
        new ProjectNode(
            "watermark_project",
            List.of(new EmptyNode(veloxRowType)),
            List.of("watermark"),
            List.of(FieldAccessTypedExpr.create(new BigIntType(), "id")));
    WatermarkPushDownSpec watermarkSpec =
        new WatermarkPushDownSpec(watermarkProject, IDLE_TIMEOUT_MS, WATERMARK_INTERVAL_MS, 0);

    Map<String, String> tableParams = new HashMap<>();
    tableParams.put("bootstrap.servers", "127.0.0.1:" + KAFKA_PORT);
    tableParams.put("client.id", "test-client-e2e-" + UUID.randomUUID());
    tableParams.put("group.id", "test-group-e2e");
    tableParams.put("topic", topic);
    tableParams.put("format", "json");
    tableParams.put("scan.startup.mode", "earliest-offsets");
    tableParams.put("enable.auto.commit", "false");

    KafkaTableHandle tableHandle =
        new KafkaTableHandle("connector-kafka", topic, veloxRowType, tableParams);
    String planId = "plan_" + UUID.randomUUID().toString().replace("-", "");
    TableScanWithWatermarkNode scanNode =
        new TableScanWithWatermarkNode(planId, veloxRowType, tableHandle, List.of(), watermarkSpec);
    KafkaConnectorSplit connectorSplit =
        new KafkaConnectorSplit(
            "connector-kafka",
            0,
            false,
            "127.0.0.1:" + KAFKA_PORT,
            "test-group-e2e",
            "json",
            false,
            "earliest-offset",
            List.of(new KafkaConnectorSplit.TopicPartitionOffset(topic, 0, -1L)));

    GlutenSourceFunction<StatefulRecord> sourceFn =
        new GlutenSourceFunction<>(
            new StatefulPlanNode(scanNode.getId(), scanNode),
            Map.of(scanNode.getId(), veloxRowType),
            scanNode.getId(),
            connectorSplit,
            StatefulRecord.class);
    sourceFn.setShouldCallNoMoreSplits(false);

    GlutenStreamSource sourceOp = new GlutenStreamSource(sourceFn, "KafkaSource");
    return new DataStreamSource<StatefulRecord>(
        env, TypeInformation.of(StatefulRecord.class), sourceOp, false, "KafkaSource");
  }

  private static ProducerRecord<byte[], byte[]> jsonRecord(String topic, String value) {
    return new ProducerRecord<>(topic, value.getBytes(StandardCharsets.UTF_8));
  }

  // -- Capture operator --

  private static class StatusCaptureOp extends AbstractStreamOperator<StatefulRecord>
      implements OneInputStreamOperator<StatefulRecord, StatefulRecord> {

    @Override
    public void processElement(StreamRecord<StatefulRecord> element) throws Exception {
      output.collect(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
      output.emitWatermark(mark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus status) throws Exception {
      capturedStatuses.add(status);
      super.processWatermarkStatus(status);
    }
  }
}
