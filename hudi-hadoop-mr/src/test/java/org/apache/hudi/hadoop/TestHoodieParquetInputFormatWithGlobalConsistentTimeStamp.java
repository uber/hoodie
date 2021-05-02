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

package org.apache.hudi.hadoop;

import java.util.Arrays;
import java.util.Collection;
import org.apache.hudi.common.table.HoodieTableGloballyConsistentMetaClient;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

public class TestHoodieParquetInputFormatWithGlobalConsistentTimeStamp {
    //extends TestHoodieParquetInputFormat {

  /*// Test parquet input format using both session property and timestamp property
  private String timeStampAndSession;

  public TestHoodieParquetInputFormatWithGlobalConsistentTimeStamp(String timeStampAndSession) {
    this.timeStampAndSession = timeStampAndSession;
  }
*/
  @Test
  public void r() {}

  /*@Parameterized.Parameters(name = "timeStampAndSession")
  public static Collection<String[]> data() {
    // 00 not needed thats the parent class unit test itself
    return Arrays.asList(new String[][] {{"01"}, {"10"}, {"11"}});
  }

  @Override
  public void setUp() {
    super.setUp();
    switch (timeStampAndSession) {
      case "01":
        jobConf.setBoolean(HoodieTableGloballyConsistentMetaClient.DISABLE_HOODIE_GLOBALLY_CONSISTENT_READS,
            true);
        break;
      case "10":
        jobConf.set(HoodieTableGloballyConsistentMetaClient.GLOBALLY_CONSISTENT_READ_TIMESTAMP,
            String.valueOf(Long.MAX_VALUE));
        break;
      case "11":
        // set to 0 attempting to hide everything but this won't work due to session property
        jobConf.set(HoodieTableGloballyConsistentMetaClient.GLOBALLY_CONSISTENT_READ_TIMESTAMP, "0");
        jobConf.setBoolean(HoodieTableGloballyConsistentMetaClient.DISABLE_HOODIE_GLOBALLY_CONSISTENT_READS,
            true);
        break;
      default:
        throw new RuntimeException(
            String.format("unexpected timestampAndSession value: %s", timeStampAndSession));

    }
  }*/
}
