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

package org.apache.hudi.client.bootstrap;

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.selector.RecordDataBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.RecordMetadataOnlyBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.UniBootstrapModeSelector;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUniBootstrapModeSelector {

  @Test
  public void testFullBootstrapModeSelector() {

    RecordDataBootstrapModeSelector modeSelector = new RecordDataBootstrapModeSelector(
        HoodieWriteConfig.newBuilder().withPath("").build());
    testModeSelector(modeSelector, BootstrapMode.RECORD_DATA_BOOTSTRAP);
  }

  @Test
  public void testMetadataOnlyBootstrapModeSelector() {
    RecordMetadataOnlyBootstrapModeSelector modeSelector = new RecordMetadataOnlyBootstrapModeSelector(
        HoodieWriteConfig.newBuilder().withPath("").build());
    testModeSelector(modeSelector, BootstrapMode.RECORD_METADATA_ONLY_BOOTSTRAP);
  }

  private void testModeSelector(UniBootstrapModeSelector modeSelector, BootstrapMode mode) {
    List<String> partitionPaths = Arrays.asList("2020/05/01", "2020/05/02", "2020/05/10", "2020/05/11");
    List<Pair<String, List<HoodieFileStatus>>> input = partitionPaths.stream()
        .map(p -> Pair.<String, List<HoodieFileStatus>>of(p, new ArrayList<>())).collect(Collectors.toList());
    Map<BootstrapMode, List<String>> result = modeSelector.select(input);
    assertTrue(result.get(mode).contains("2020/05/01"));
    assertTrue(result.get(mode).contains("2020/05/02"));
    assertTrue(result.get(mode).contains("2020/05/10"));
    assertTrue(result.get(mode).contains("2020/05/11"));
    assertEquals(4, result.get(mode).size());
  }
}
