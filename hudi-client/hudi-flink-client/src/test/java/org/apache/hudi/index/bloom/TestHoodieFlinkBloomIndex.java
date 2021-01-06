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

package org.apache.hudi.index.bloom;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.*;

public class TestHoodieFlinkBloomIndex extends org.apache.hudi.testutils.HoodieFlinkClientTestHarness {

  private static final org.apache.avro.Schema SCHEMA = getSchemaFromResource(TestHoodieFlinkBloomIndex.class, "/exampleSchema.avsc", true);
  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with rangePruning={0}, treeFiltering={1}, bucketizedChecking={2}";

  public static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> configParams() {
    Object[][] data =
        new Object[][] {{true, true, true}, {false, true, true}, {true, true, false}, {true, false, true}};
    return java.util.stream.Stream.of(data).map(org.junit.jupiter.params.provider.Arguments::of);
  }

  @org.junit.jupiter.api.BeforeEach
  public void setUp() throws Exception {
    initFlinkContexts();
    initPath();
    initFileSystem();
    // We have some records to be tagged (two different partitions)
    initMetaClient();
  }

  @org.junit.jupiter.api.AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private org.apache.hudi.config.HoodieWriteConfig makeConfig(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) {
    return org.apache.hudi.config.HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(org.apache.hudi.config.HoodieIndexConfig.newBuilder().bloomIndexPruneByRanges(rangePruning)
            .bloomIndexTreebasedFilter(treeFiltering).bloomIndexBucketizedChecking(bucketizedChecking)
            .bloomIndexKeysPerBucket(2).build())
        .build();
  }

  @org.junit.jupiter.params.ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @org.junit.jupiter.params.provider.MethodSource("configParams")
  public void testLoadInvolvedFiles(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) throws Exception {
    org.apache.hudi.config.HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    FlinkHoodieBloomIndex index = new FlinkHoodieBloomIndex(config);
    HoodieTable hoodieTable = HoodieFlinkTable.create(config, context, metaClient);
    org.apache.hudi.testutils.HoodieFlinkWriteableTestTable testTable = org.apache.hudi.testutils.HoodieFlinkWriteableTestTable.of(hoodieTable, SCHEMA);

    // Create some partitions, and put some files
    // "2016/01/21": 0 file
    // "2016/04/01": 1 file (2_0_20160401010101.parquet)
    // "2015/03/12": 3 files (1_0_20150312101010.parquet, 3_0_20150312101010.parquet, 4_0_20150312101010.parquet)
    testTable.withPartitionMetaFiles("2016/01/21", "2016/04/01", "2015/03/12");

    RawTripTestPayload rowChange1 =
        new RawTripTestPayload("{\"_row_key\":\"000\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record1 =
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    RawTripTestPayload rowChange2 =
        new RawTripTestPayload("{\"_row_key\":\"001\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record2 =
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
    RawTripTestPayload rowChange3 =
        new RawTripTestPayload("{\"_row_key\":\"002\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record3 =
        new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
    RawTripTestPayload rowChange4 =
        new RawTripTestPayload("{\"_row_key\":\"003\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}");
    HoodieRecord record4 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    java.util.List<String> partitions = asList("2016/01/21", "2016/04/01", "2015/03/12");
    java.util.List<scala.Tuple2<String, BloomIndexFileInfo>> filesList = index.loadInvolvedFiles(partitions, context, hoodieTable);
    // Still 0, as no valid commit
    assertEquals(0, filesList.size());

    testTable.addCommit("20160401010101").withInserts("2016/04/01", "2");
    testTable.addCommit("20150312101010").withInserts("2015/03/12", "1")
        .withInserts("2015/03/12", "3", record1)
        .withInserts("2015/03/12", "4", record2, record3, record4);
    metaClient.reloadActiveTimeline();

    filesList = index.loadInvolvedFiles(partitions, context, hoodieTable);
    assertEquals(4, filesList.size());

    if (rangePruning) {
      // these files will not have the key ranges
      assertNull(filesList.get(0)._2().getMaxRecordKey());
      assertNull(filesList.get(0)._2().getMinRecordKey());
      assertFalse(filesList.get(1)._2().hasKeyRanges());
      assertNotNull(filesList.get(2)._2().getMaxRecordKey());
      assertNotNull(filesList.get(2)._2().getMinRecordKey());
      assertTrue(filesList.get(3)._2().hasKeyRanges());

      // no longer sorted, but should have same files.

      java.util.List<scala.Tuple2<String, BloomIndexFileInfo>> expected =
          asList(new Tuple2<>("2016/04/01", new BloomIndexFileInfo("2")),
              new Tuple2<>("2015/03/12", new BloomIndexFileInfo("1")),
              new Tuple2<>("2015/03/12", new BloomIndexFileInfo("3", "000", "000")),
              new Tuple2<>("2015/03/12", new BloomIndexFileInfo("4", "001", "003")));
      assertEquals(expected, filesList);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @org.junit.jupiter.params.provider.MethodSource("configParams")
  public void testRangePruning(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) {
    org.apache.hudi.config.HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    FlinkHoodieBloomIndex index = new FlinkHoodieBloomIndex(config);

    final java.util.Map<String, java.util.List<BloomIndexFileInfo>> partitionToFileIndexInfo = new HashMap<>();
    partitionToFileIndexInfo.put("2017/10/22",
        asList(new BloomIndexFileInfo("f1"), new BloomIndexFileInfo("f2", "000", "000"),
            new BloomIndexFileInfo("f3", "001", "003"), new BloomIndexFileInfo("f4", "002", "007"),
            new BloomIndexFileInfo("f5", "009", "010")));

    Map<String, List<String>> partitionRecordKeyMap = new HashMap<>();
    asList(new Tuple2<>("2017/10/22", "003"), new Tuple2<>("2017/10/22", "002"),
            new Tuple2<>("2017/10/22", "005"), new Tuple2<>("2017/10/22", "004"))
            .forEach( t -> {
                List<String> recordKeyList = partitionRecordKeyMap.getOrDefault(t._1, new ArrayList<>());
                recordKeyList.add(t._2);
                partitionRecordKeyMap.put(t._1, recordKeyList);
            });

    List<scala.Tuple2<String, HoodieKey>> comparisonKeyList =
        index.explodeRecordsWithFileComparisons(partitionToFileIndexInfo, partitionRecordKeyMap);

    assertEquals(10, comparisonKeyList.size());
    java.util.Map<String, java.util.List<String>> recordKeyToFileComps = comparisonKeyList.stream()
        .collect(java.util.stream.Collectors.groupingBy(t -> t._2.getRecordKey(), java.util.stream.Collectors.mapping(t -> t._1, java.util.stream.Collectors.toList())));

    assertEquals(4, recordKeyToFileComps.size());
    assertEquals(new java.util.HashSet<>(asList("f1", "f3", "f4")), new java.util.HashSet<>(recordKeyToFileComps.get("002")));
    assertEquals(new java.util.HashSet<>(asList("f1", "f3", "f4")), new java.util.HashSet<>(recordKeyToFileComps.get("003")));
    assertEquals(new java.util.HashSet<>(asList("f1", "f4")), new java.util.HashSet<>(recordKeyToFileComps.get("004")));
    assertEquals(new java.util.HashSet<>(asList("f1", "f4")), new java.util.HashSet<>(recordKeyToFileComps.get("005")));
  }

  @org.junit.jupiter.api.Test
  public void testCheckUUIDsAgainstOneFile() throws Exception {
    final String partition = "2016/01/31";
    // Create some records to use
    String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    String recordStr4 = "{\"_row_key\":\"4eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":32}";
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    HoodieRecord record1 =
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    HoodieRecord record2 =
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
    RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
    HoodieRecord record3 =
        new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
    RawTripTestPayload rowChange4 = new RawTripTestPayload(recordStr4);
    HoodieRecord record4 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);

    // We write record1, record2 to a parquet file, but the bloom filter contains (record1,
    // record2, record3).
    org.apache.hudi.common.bloom.BloomFilter filter = org.apache.hudi.common.bloom.BloomFilterFactory.createBloomFilter(10000, 0.0000001, -1, org.apache.hudi.common.bloom.BloomFilterTypeCode.SIMPLE.name());
    filter.add(record3.getRecordKey());
    org.apache.hudi.testutils.HoodieFlinkWriteableTestTable testTable = org.apache.hudi.testutils.HoodieFlinkWriteableTestTable.of(metaClient, SCHEMA, filter);
    String fileId = testTable.addCommit("000").getFileIdWithInserts(partition, record1, record2);
    String filename = testTable.getBaseFileNameById(fileId);

    // The bloom filter contains 3 records
    assertTrue(filter.mightContain(record1.getRecordKey()));
    assertTrue(filter.mightContain(record2.getRecordKey()));
    assertTrue(filter.mightContain(record3.getRecordKey()));
    assertFalse(filter.mightContain(record4.getRecordKey()));

    // Compare with file
    java.util.List<String> uuids =
        asList(record1.getRecordKey(), record2.getRecordKey(), record3.getRecordKey(), record4.getRecordKey());

    org.apache.hudi.config.HoodieWriteConfig config = org.apache.hudi.config.HoodieWriteConfig.newBuilder().withPath(basePath).build();
    HoodieFlinkTable table = HoodieFlinkTable.create(config, context, metaClient);
    org.apache.hudi.io.HoodieKeyLookupHandle keyHandle = new org.apache.hudi.io.HoodieKeyLookupHandle<>(config, table, Pair.of(partition, fileId));
    java.util.List<String> results = keyHandle.checkCandidatesAgainstFile(hadoopConf, uuids,
        new org.apache.hadoop.fs.Path(java.nio.file.Paths.get(basePath, partition, filename).toString()));
    assertEquals(results.size(), 2);
    assertTrue(results.get(0).equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")
        || results.get(1).equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0"));
    assertTrue(results.get(0).equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")
        || results.get(1).equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0"));
    // TODO(vc): Need more coverage on actual filenames
    // assertTrue(results.get(0)._2().equals(filename));
    // assertTrue(results.get(1)._2().equals(filename));
  }

  @org.junit.jupiter.params.ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @org.junit.jupiter.params.provider.MethodSource("configParams")
  public void testTagLocationWithEmptyList(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) {
    // We have some records to be tagged (two different partitions)
    List<HoodieRecord> records = new ArrayList<>();
    // Also create the metadata and config
    org.apache.hudi.config.HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    metaClient = org.apache.hudi.common.table.HoodieTableMetaClient.reload(metaClient);
    HoodieFlinkTable table = HoodieFlinkTable.create(config, context, metaClient);

    // Let's tag
    FlinkHoodieBloomIndex bloomIndex = new FlinkHoodieBloomIndex(config);

    assertDoesNotThrow(() -> {
      bloomIndex.tagLocation(records, context, table);
    }, "EmptyList should not result in IllegalArgumentException: Positive number of slices required");
  }

  @org.junit.jupiter.params.ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @org.junit.jupiter.params.provider.MethodSource("configParams")
  public void testTagLocation(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) throws Exception {
    // We have some records to be tagged (two different partitions)
    String rowKey1 = java.util.UUID.randomUUID().toString();
    String rowKey2 = java.util.UUID.randomUUID().toString();
    String rowKey3 = java.util.UUID.randomUUID().toString();
    String recordStr1 = "{\"_row_key\":\"" + rowKey1 + "\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"" + rowKey2 + "\",\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"" + rowKey3 + "\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    // place same row key under a different partition.
    String recordStr4 = "{\"_row_key\":\"" + rowKey1 + "\",\"time\":\"2015-01-31T03:16:41.415Z\",\"number\":32}";
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    HoodieRecord record1 =
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    HoodieRecord record2 =
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);
    RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
    HoodieRecord record3 =
        new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3);
    RawTripTestPayload rowChange4 = new RawTripTestPayload(recordStr4);
    HoodieRecord record4 =
        new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()), rowChange4);
    List<HoodieRecord> records = asList(record1, record2, record3, record4);

    // Also create the metadata and config
    org.apache.hudi.config.HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    HoodieFlinkTable hoodieTable = HoodieFlinkTable.create(config, context, metaClient);
    org.apache.hudi.testutils.HoodieFlinkWriteableTestTable testTable = org.apache.hudi.testutils.HoodieFlinkWriteableTestTable.of(hoodieTable, SCHEMA);

    // Let's tag
    FlinkHoodieBloomIndex bloomIndex = new FlinkHoodieBloomIndex(config);
    List<HoodieRecord> taggedRecords = bloomIndex.tagLocation(records, context, hoodieTable);

    // Should not find any files
    for (HoodieRecord record : taggedRecords) {
      assertFalse(record.isCurrentLocationKnown());
    }

    // We create three parquet file, each having one record. (two different partitions)
    String fileId1 = testTable.addCommit("001").getFileIdWithInserts("2016/01/31", record1);
    String fileId2 = testTable.addCommit("002").getFileIdWithInserts("2016/01/31", record2);
    String fileId3 = testTable.addCommit("003").getFileIdWithInserts("2015/01/31", record4);

    metaClient.reloadActiveTimeline();

    // We do the tag again
    taggedRecords = bloomIndex.tagLocation(records, context, HoodieFlinkTable.create(config, context, metaClient));

    // Check results
    for (HoodieRecord record : taggedRecords) {
      if (record.getRecordKey().equals(rowKey1)) {
        if (record.getPartitionPath().equals("2015/01/31")) {
          assertEquals(record.getCurrentLocation().getFileId(), fileId3);
        } else {
          assertEquals(record.getCurrentLocation().getFileId(), fileId1);
        }
      } else if (record.getRecordKey().equals(rowKey2)) {
        assertEquals(record.getCurrentLocation().getFileId(), fileId2);
      } else if (record.getRecordKey().equals(rowKey3)) {
        assertFalse(record.isCurrentLocationKnown());
      }
    }
  }

  @org.junit.jupiter.params.ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @org.junit.jupiter.params.provider.MethodSource("configParams")
  public void testCheckExists(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) throws Exception {
    // We have some records to be tagged (two different partitions)

    String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    // record key same as recordStr2
    String recordStr4 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2015-01-31T03:16:41.415Z\",\"number\":32}";
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    HoodieKey key1 = new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath());
    HoodieRecord record1 = new HoodieRecord(key1, rowChange1);
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    HoodieKey key2 = new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath());
    HoodieRecord record2 = new HoodieRecord(key2, rowChange2);
    RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
    HoodieKey key3 = new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath());
    RawTripTestPayload rowChange4 = new RawTripTestPayload(recordStr4);
    HoodieKey key4 = new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath());
    HoodieRecord record4 = new HoodieRecord(key4, rowChange4);
    List<HoodieKey> keys = asList(key1, key2, key3, key4);

    // Also create the metadata and config
    org.apache.hudi.config.HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    org.apache.hudi.table.HoodieTable hoodieTable = HoodieFlinkTable.create(config, context, metaClient);
    org.apache.hudi.testutils.HoodieFlinkWriteableTestTable testTable = org.apache.hudi.testutils.HoodieFlinkWriteableTestTable.of(hoodieTable, SCHEMA);

    // Let's tag
    FlinkHoodieBloomIndex bloomIndex = new FlinkHoodieBloomIndex(config);
    List<HoodieRecord> toTagRecords = new ArrayList<>();
    toTagRecords.add(new HoodieRecord(record4.getKey(), null));
    List<HoodieRecord> taggedRecords = bloomIndex.tagLocation(toTagRecords, context, hoodieTable);
    Map<HoodieKey, Option<Pair<String, String>>> recordLocations = new HashMap<>();
    for (HoodieRecord taggedRecord : taggedRecords) {
      recordLocations.put(taggedRecord.getKey(), taggedRecord.isCurrentLocationKnown()
                ? org.apache.hudi.common.util.Option.of(Pair.of(taggedRecord.getPartitionPath(), taggedRecord.getCurrentLocation().getFileId()))
                : org.apache.hudi.common.util.Option.empty());
    }
      // Should not find any files
    for (Option<Pair<String, String>> record : recordLocations.values()) {
      assertTrue(!record.isPresent());
    }

    // We create three parquet file, each having one record. (two different partitions)
    String fileId1 = testTable.addCommit("001").getFileIdWithInserts("2016/01/31", record1);
    String fileId2 = testTable.addCommit("002").getFileIdWithInserts("2016/01/31", record2);
    String fileId3 = testTable.addCommit("003").getFileIdWithInserts("2015/01/31", record4);

    // We do the tag again
    metaClient = org.apache.hudi.common.table.HoodieTableMetaClient.reload(metaClient);
    hoodieTable = HoodieFlinkTable.create(config, context, metaClient);
    List<HoodieRecord> toTagRecords1 = new ArrayList<>();
    for (HoodieKey key : keys) {
        taggedRecords.add(new org.apache.hudi.common.model.HoodieRecord(key, null));
    }

    taggedRecords = bloomIndex.tagLocation(toTagRecords1, context, hoodieTable);
    recordLocations.clear();
    for (org.apache.hudi.common.model.HoodieRecord taggedRecord : taggedRecords) {
      recordLocations.put(taggedRecord.getKey(), taggedRecord.isCurrentLocationKnown()
                ? org.apache.hudi.common.util.Option.of(org.apache.hudi.common.util.collection.Pair.of(taggedRecord.getPartitionPath(), taggedRecord.getCurrentLocation().getFileId()))
                : org.apache.hudi.common.util.Option.empty());
    }

      // Check results
    for (Map.Entry<HoodieKey, Option<Pair<String, String>>> record : recordLocations.entrySet()) {
      if (record.getKey().getRecordKey().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")) {
        assertTrue(record.getValue().isPresent());
        assertEquals(fileId1, record.getValue().get().getRight());
      } else if (record.getKey().getRecordKey().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")) {
        assertTrue(record.getValue().isPresent());
        if (record.getKey().getPartitionPath().equals("2015/01/31")) {
          assertEquals(fileId3, record.getValue().get().getRight());
        } else {
          assertEquals(fileId2, record.getValue().get().getRight());
        }
      } else if (record.getKey().getRecordKey().equals("3eb5b87c-1fej-4edd-87b4-6ec96dc405a0")) {
        assertFalse(record.getValue().isPresent());
      }
    }
  }

  @org.junit.jupiter.params.ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @org.junit.jupiter.params.provider.MethodSource("configParams")
  public void testBloomFilterFalseError(boolean rangePruning, boolean treeFiltering, boolean bucketizedChecking) throws Exception {
    // We have two hoodie records
    String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";

    // We write record1 to a parquet file, using a bloom filter having both records
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    HoodieRecord record1 =
        new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    HoodieRecord record2 =
        new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2);

    org.apache.hudi.common.bloom.BloomFilter filter = org.apache.hudi.common.bloom.BloomFilterFactory.createBloomFilter(10000, 0.0000001, -1,
        org.apache.hudi.common.bloom.BloomFilterTypeCode.SIMPLE.name());
    filter.add(record2.getRecordKey());
    org.apache.hudi.testutils.HoodieFlinkWriteableTestTable testTable = org.apache.hudi.testutils.HoodieFlinkWriteableTestTable.of(metaClient, SCHEMA, filter);
    String fileId = testTable.addCommit("000").getFileIdWithInserts("2016/01/31", record1);
    assertTrue(filter.mightContain(record1.getRecordKey()));
    assertTrue(filter.mightContain(record2.getRecordKey()));

    // We do the tag
    List<HoodieRecord> records = asList(record1, record2);
    org.apache.hudi.config.HoodieWriteConfig config = makeConfig(rangePruning, treeFiltering, bucketizedChecking);
    metaClient = org.apache.hudi.common.table.HoodieTableMetaClient.reload(metaClient);
    org.apache.hudi.table.HoodieTable table = HoodieFlinkTable.create(config, context, metaClient);

    FlinkHoodieBloomIndex bloomIndex = new FlinkHoodieBloomIndex(config);
    List<HoodieRecord> taggedRecords = bloomIndex.tagLocation(records, context, table);

    // Check results
    for (HoodieRecord record : taggedRecords) {
      if (record.getKey().equals("1eb5b87a-1feh-4edd-87b4-6ec96dc405a0")) {
        assertEquals(record.getCurrentLocation().getFileId(), fileId);
      } else if (record.getRecordKey().equals("2eb5b87b-1feu-4edd-87b4-6ec96dc405a0")) {
        assertFalse(record.isCurrentLocationKnown());
      }
    }
  }
}
