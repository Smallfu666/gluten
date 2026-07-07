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

import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.FromElementsConnectorSplit;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.stateful.StatefulWatermarkStatus;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link GlutenSourceFunction} watermark status (IDLE/ACTIVE) handling. */
public class GlutenSourceFunctionWatermarkStatusTest {

  private static final String NODE_ID = "test-node";
  private static final String CONNECTOR_ID = "test-connector";

  private static final RowType OUTPUT_TYPE =
      new RowType(Arrays.asList("id"), Arrays.asList(new BigIntType()));

  /**
   * A test spy that tracks {@link SourceContext} invocations so we can verify that idle/active
   * transitions happen correctly.
   */
  private static class TrackingSourceContext<T> implements SourceContext<T> {
    final List<String> invocations = new ArrayList<>();
    final List<T> collected = new ArrayList<>();
    final List<Watermark> watermarks = new ArrayList<>();
    boolean idleCalled = false;

    @Override
    public void collect(T element) {
      invocations.add("collect");
      collected.add(element);
    }

    @Override
    public void collectWithTimestamp(T element, long timestamp) {
      invocations.add("collectWithTimestamp");
      collected.add(element);
    }

    @Override
    public void emitWatermark(Watermark mark) {
      invocations.add("emitWatermark");
      watermarks.add(mark);
    }

    @Override
    public void markAsTemporarilyIdle() {
      invocations.add("markAsTemporarilyIdle");
      idleCalled = true;
    }

    @Override
    public Object getCheckpointLock() {
      return this;
    }

    @Override
    public void close() {}
  }

  // ─── createMinimalSourceFunction ────────────────────────────────────────

  private GlutenSourceFunction<RowData> createSourceFunction() {
    ConnectorSplit split = new FromElementsConnectorSplit(CONNECTOR_ID, 0, false);
    return new GlutenSourceFunction<>(
        new StatefulPlanNode(NODE_ID, new EmptyNode(OUTPUT_TYPE)),
        Map.of(NODE_ID, OUTPUT_TYPE),
        NODE_ID,
        split,
        RowData.class);
  }

  // ─── tests ──────────────────────────────────────────────────────────────

  @Test
  public void testIdleWatermarkStatusCallsMarkAsTemporarilyIdle() throws Exception {
    GlutenSourceFunction<RowData> sourceFunction = createSourceFunction();
    TrackingSourceContext<RowData> context = new TrackingSourceContext<>();
    StatefulWatermarkStatus idleStatus = new StatefulWatermarkStatus(NODE_ID, true);

    invokeProcessWatermarkStatus(sourceFunction, context, idleStatus);

    assertTrue(context.idleCalled, "IDLE status should trigger markAsTemporarilyIdle()");
  }

  @Test
  public void testActiveWatermarkStatusDoesNotCallMarkAsTemporarilyIdle() throws Exception {
    GlutenSourceFunction<RowData> sourceFunction = createSourceFunction();
    TrackingSourceContext<RowData> context = new TrackingSourceContext<>();
    StatefulWatermarkStatus activeStatus = new StatefulWatermarkStatus(NODE_ID, false);

    invokeProcessWatermarkStatus(sourceFunction, context, activeStatus);

    assertFalse(context.idleCalled, "ACTIVE status should NOT trigger markAsTemporarilyIdle()");
  }

  @Test
  public void testIdleThenActiveTransition() throws Exception {
    // Verifies that after receiving IDLE, subsequent ACTIVE does not call markAsTemporarilyIdle.
    // The ACTIVE status is emitted by native when data resumes; Flink reactivates the source
    // when the next record/watermark is collected.
    GlutenSourceFunction<RowData> sourceFunction = createSourceFunction();
    TrackingSourceContext<RowData> context = new TrackingSourceContext<>();

    StatefulWatermarkStatus idleStatus = new StatefulWatermarkStatus(NODE_ID, true);
    StatefulWatermarkStatus activeStatus = new StatefulWatermarkStatus(NODE_ID, false);

    invokeProcessWatermarkStatus(sourceFunction, context, idleStatus);
    assertTrue(context.idleCalled);

    context.idleCalled = false;
    invokeProcessWatermarkStatus(sourceFunction, context, activeStatus);
    assertFalse(context.idleCalled);

    // SourceContext state is not visible from our spy — verify invocations sequence.
    assertThat(context.invocations).containsExactly("markAsTemporarilyIdle");
  }

  @Test
  public void testRepeatedIdleCallsAreIdempotent() throws Exception {
    // Multiple IDLE signals should each call markAsTemporarilyIdle — it is safe to
    // call it multiple times (Flink's implementation is idempotent).
    GlutenSourceFunction<RowData> sourceFunction = createSourceFunction();
    TrackingSourceContext<RowData> context = new TrackingSourceContext<>();

    StatefulWatermarkStatus idleStatus = new StatefulWatermarkStatus(NODE_ID, true);

    invokeProcessWatermarkStatus(sourceFunction, context, idleStatus);
    invokeProcessWatermarkStatus(sourceFunction, context, idleStatus);
    invokeProcessWatermarkStatus(sourceFunction, context, idleStatus);

    assertThat(context.invocations)
        .containsExactly("markAsTemporarilyIdle", "markAsTemporarilyIdle", "markAsTemporarilyIdle");
  }

  @Test
  public void testActiveWatermarkStatusIsNoOp() throws Exception {
    // ACTIVE status should produce no invocations on the SourceContext.
    GlutenSourceFunction<RowData> sourceFunction = createSourceFunction();
    TrackingSourceContext<RowData> context = new TrackingSourceContext<>();

    StatefulWatermarkStatus activeStatus = new StatefulWatermarkStatus(NODE_ID, false);

    invokeProcessWatermarkStatus(sourceFunction, context, activeStatus);

    assertThat(context.invocations).isEmpty();
  }

  // ─── reflection helper ──────────────────────────────────────────────────

  private void invokeProcessWatermarkStatus(
      GlutenSourceFunction<?> sourceFunction,
      SourceContext<?> context,
      StatefulWatermarkStatus status)
      throws Exception {
    Method method =
        GlutenSourceFunction.class.getDeclaredMethod(
            "processWatermarkStatus", SourceContext.class, StatefulWatermarkStatus.class);
    method.setAccessible(true);
    method.invoke(sourceFunction, context, status);
  }
}
