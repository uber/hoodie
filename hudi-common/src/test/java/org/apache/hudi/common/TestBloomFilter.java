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

package org.apache.hudi.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests bloom filter {@link BloomFilter}.
 */
@RunWith(Parameterized.class)
public class TestBloomFilter {

  private final int keySize = 50;
  private final int versionToTest;

  // name attribute is optional, provide an unique name for test
  // multiple parameters, uses Collection<Object[]>
  @Parameters()
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {0},
        {1}
    });
  }

  public TestBloomFilter(int versionToTest) {
    this.versionToTest = versionToTest;
  }

  @Test
  public void testAddKey() {
    List<java.lang.String> inputs = new ArrayList<>();
    int[] sizes = {100, 1000, 10000};
    for (int size : sizes) {
      inputs = new ArrayList<>();
      BloomFilter filter = BloomFilterFactory
          .createBloomFilter(100, 0.0000001, versionToTest);
      for (int i = 0; i < size; i++) {
        java.lang.String key = RandomStringUtils.randomAlphanumeric(keySize);
        inputs.add(key);
        filter.add(key);
      }
      for (java.lang.String key : inputs) {
        assert (filter.mightContain(key));
      }
      for (int i = 0; i < 100; i++) {
        java.lang.String randomKey = RandomStringUtils.randomAlphanumeric(keySize);
        if (inputs.contains(randomKey)) {
          assert filter.mightContain(randomKey);
        }
      }
    }
  }

  @Test
  public void testSerialize() throws IOException, ClassNotFoundException {

    List<java.lang.String> inputs = new ArrayList<>();
    int[] sizes = {100, 1000, 10000};
    for (int size : sizes) {
      inputs = new ArrayList<>();
      BloomFilter filter = BloomFilterFactory
          .createBloomFilter(100, 0.0000001, versionToTest);
      for (int i = 0; i < size; i++) {
        java.lang.String key = RandomStringUtils.randomAlphanumeric(keySize);
        inputs.add(key);
        filter.add(key);
      }

      String serString = filter.serializeToString();
      BloomFilter recreatedBloomFilter = BloomFilterFactory
          .getBloomFilterFromSerializedString(serString, versionToTest);
      for (String key : inputs) {
        assert (recreatedBloomFilter.mightContain(key));
      }
    }
  }
}
