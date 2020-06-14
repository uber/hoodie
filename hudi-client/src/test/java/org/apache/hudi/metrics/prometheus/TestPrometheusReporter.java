/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics.prometheus;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.MetricsReporterType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPrometheusReporter {

  HoodieWriteConfig config = mock(HoodieWriteConfig.class);

  @Test
  public void testRegisterGauge() {
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.PROMETHEUS);
    when(config.getPrometheusHost()).thenReturn("172.16.16.80");
    when(config.getPrometheusPort()).thenReturn(9090);
    assertDoesNotThrow(() -> {
      new HoodieMetrics(config, "raw_table");
    });
  }
}