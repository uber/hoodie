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

package org.apache.hudi.common.fs;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests file system utils.
 */
public class TestFSUtils extends HoodieCommonTestHarness {

  private final long minRollbackToKeep = 10;
  private final long minCleanToKeep = 10;

  private static String TEST_WRITE_TOKEN = "1-0-1";

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
  }

  @Test
  public void testMakeBaseFileName() {
    String instantTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    String fileName = UUID.randomUUID().toString();
    assertEquals(FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName), fileName + "_" + TEST_WRITE_TOKEN + "_" + instantTime + ".parquet");
  }

  @Test
  public void testMaskFileName() {
    String instantTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    int taskPartitionId = 2;
    assertEquals(FSUtils.maskWithoutFileId(instantTime, taskPartitionId), "*_" + taskPartitionId + "_" + instantTime + ".parquet");
  }

  @Test
  /**
   * Tests if process Files return only paths excluding marker directories Cleaner, Rollback and compaction-scheduling
   * logic was recursively processing all subfolders including that of ".hoodie" when looking for partition-paths. This
   * causes a race when they try to list all folders (recursively) but the marker directory (that of compaction inside
   * of ".hoodie" folder) is deleted underneath by compactor. This code tests the fix by ensuring ".hoodie" and their
   * subfolders are never processed.
   */
  public void testProcessFiles() throws Exception {
    // All directories including marker dirs.
    List<String> folders =
        Arrays.asList("2016/04/15", "2016/05/16", ".hoodie/.temp/2/2016/04/15", ".hoodie/.temp/2/2016/05/16");
    folders.forEach(f -> {
      try {
        metaClient.getFs().mkdirs(new Path(new Path(basePath), f));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Files inside partitions and marker directories
    List<String> files = Arrays.asList("2016/04/15/1_1-0-1_20190528120000.parquet",
        "2016/05/16/2_1-0-1_20190528120000.parquet", ".hoodie/.temp/2/2016/05/16/2_1-0-1_20190528120000.parquet",
        ".hoodie/.temp/2/2016/04/15/1_1-0-1_20190528120000.parquet");

    files.forEach(f -> {
      try {
        metaClient.getFs().create(new Path(new Path(basePath), f));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Test excluding meta-folder
    final List<String> collected = new ArrayList<>();
    FSUtils.processFiles(metaClient.getFs(), basePath, (status) -> {
      collected.add(status.getPath().toString());
      return true;
    }, true);

    assertTrue(collected.stream().noneMatch(s -> s.contains(HoodieTableMetaClient.METAFOLDER_NAME)),
        "Hoodie MetaFolder MUST be skipped but got :" + collected);
    // Check if only files are listed
    assertEquals(2, collected.size());

    // Test including meta-folder
    final List<String> collected2 = new ArrayList<>();
    FSUtils.processFiles(metaClient.getFs(), basePath, (status) -> {
      collected2.add(status.getPath().toString());
      return true;
    }, false);

    assertFalse(collected2.stream().noneMatch(s -> s.contains(HoodieTableMetaClient.METAFOLDER_NAME)),
        "Hoodie MetaFolder will be present :" + collected2);
    // Check if only files are listed including hoodie.properties
    assertEquals(5, collected2.size(), "Collected=" + collected2);
  }

  @Test
  public void testGetCommitTime() {
    String instantTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    String fileName = UUID.randomUUID().toString();
    String fullFileName = FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName);
    assertEquals(instantTime, FSUtils.getCommitTime(fullFileName));
  }

  @Test
  public void testGetFileNameWithoutMeta() {
    String instantTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    String fileName = UUID.randomUUID().toString();
    String fullFileName = FSUtils.makeBaseFileName(instantTime, TEST_WRITE_TOKEN, fileName);
    assertEquals(fileName, FSUtils.getFileId(fullFileName));
  }

  @Test
  public void testEnvVarVariablesPickedup() {
    environmentVariables.set("HOODIE_ENV_fs_DOT_key1", "value1");
    Configuration conf = FSUtils.prepareHadoopConf(HoodieTestUtils.getDefaultHadoopConf());
    assertEquals("value1", conf.get("fs.key1"));
    conf.set("fs.key1", "value11");
    conf.set("fs.key2", "value2");
    assertEquals("value11", conf.get("fs.key1"));
    assertEquals("value2", conf.get("fs.key2"));
  }

  @Test
  public void testGetRelativePartitionPath() {
    Path basePath = new Path("/test/apache");
    Path partitionPath = new Path("/test/apache/hudi/sub");
    assertEquals("hudi/sub", FSUtils.getRelativePartitionPath(basePath, partitionPath));
  }

  @Test
  public void testGetRelativePartitionPathSameFolder() {
    Path basePath = new Path("/test");
    Path partitionPath = new Path("/test");
    assertEquals("", FSUtils.getRelativePartitionPath(basePath, partitionPath));
  }

  @Test
  public void testGetRelativePartitionPathRepeatedFolderNameBasePath() {
    Path basePath = new Path("/test/apache/apache");
    Path partitionPath = new Path("/test/apache/apache/hudi");
    assertEquals("hudi", FSUtils.getRelativePartitionPath(basePath, partitionPath));
  }

  @Test
  public void testGetRelativePartitionPathRepeatedFolderNamePartitionPath() {
    Path basePath = new Path("/test/apache");
    Path partitionPath = new Path("/test/apache/apache/hudi");
    assertEquals("apache/hudi", FSUtils.getRelativePartitionPath(basePath, partitionPath));
  }

  @Test
  public void testOldLogFileName() {
    // Check if old log file names are still parseable by FSUtils method
    String partitionPath = "2019/01/01/";
    String fileName = UUID.randomUUID().toString();
    String oldLogFile = makeOldLogFileName(fileName, ".log", "100", 1);
    Path rlPath = new Path(new Path(partitionPath), oldLogFile);
    assertTrue(FSUtils.isLogFile(rlPath));
    assertEquals(fileName, FSUtils.getFileIdFromLogPath(rlPath));
    assertEquals("100", FSUtils.getBaseCommitTimeFromLogPath(rlPath));
    assertEquals(1, FSUtils.getFileVersionFromLog(rlPath));
    assertNull(FSUtils.getTaskPartitionIdFromLogPath(rlPath));
    assertNull(FSUtils.getStageIdFromLogPath(rlPath));
    assertNull(FSUtils.getTaskAttemptIdFromLogPath(rlPath));
    assertNull(FSUtils.getWriteTokenFromLogPath(rlPath));
  }

  @Test
  public void tesLogFileName() {
    // Check if log file names are parseable by FSUtils method
    String partitionPath = "2019/01/01/";
    String fileName = UUID.randomUUID().toString();
    String logFile = FSUtils.makeLogFileName(fileName, ".log", "100", 2, "1-0-1");
    System.out.println("Log File =" + logFile);
    Path rlPath = new Path(new Path(partitionPath), logFile);
    assertTrue(FSUtils.isLogFile(rlPath));
    assertEquals(fileName, FSUtils.getFileIdFromLogPath(rlPath));
    assertEquals("100", FSUtils.getBaseCommitTimeFromLogPath(rlPath));
    assertEquals(2, FSUtils.getFileVersionFromLog(rlPath));
    assertEquals(1, FSUtils.getTaskPartitionIdFromLogPath(rlPath));
    assertEquals(0, FSUtils.getStageIdFromLogPath(rlPath));
    assertEquals(1, FSUtils.getTaskAttemptIdFromLogPath(rlPath));
  }

  /**
   * Test Log File Comparisons when log files do not have write tokens.
   */
  @Test
  public void testOldLogFilesComparison() {
    String log1Ver0 = makeOldLogFileName("file1", ".log", "1", 0);
    String log1Ver1 = makeOldLogFileName("file1", ".log", "1", 1);
    String log1base2 = makeOldLogFileName("file1", ".log", "2", 0);
    List<HoodieLogFile> logFiles = Stream.of(log1base2, log1Ver1, log1Ver0).map(HoodieLogFile::new)
        .sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
    assertEquals(log1Ver0, logFiles.get(0).getFileName());
    assertEquals(log1Ver1, logFiles.get(1).getFileName());
    assertEquals(log1base2, logFiles.get(2).getFileName());
  }

  /**
   * Test Log File Comparisons when log files do not have write tokens.
   */
  @Test
  public void testLogFilesComparison() {
    String log1Ver0W0 = FSUtils.makeLogFileName("file1", ".log", "1", 0, "0-0-1");
    String log1Ver0W1 = FSUtils.makeLogFileName("file1", ".log", "1", 0, "1-1-1");
    String log1Ver1W0 = FSUtils.makeLogFileName("file1", ".log", "1", 1, "0-0-1");
    String log1Ver1W1 = FSUtils.makeLogFileName("file1", ".log", "1", 1, "1-1-1");
    String log1base2W0 = FSUtils.makeLogFileName("file1", ".log", "2", 0, "0-0-1");
    String log1base2W1 = FSUtils.makeLogFileName("file1", ".log", "2", 0, "1-1-1");

    List<HoodieLogFile> logFiles =
        Stream.of(log1Ver1W1, log1base2W0, log1base2W1, log1Ver1W0, log1Ver0W1, log1Ver0W0)
            .map(HoodieLogFile::new).sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
    assertEquals(log1Ver0W0, logFiles.get(0).getFileName());
    assertEquals(log1Ver0W1, logFiles.get(1).getFileName());
    assertEquals(log1Ver1W0, logFiles.get(2).getFileName());
    assertEquals(log1Ver1W1, logFiles.get(3).getFileName());
    assertEquals(log1base2W0, logFiles.get(4).getFileName());
    assertEquals(log1base2W1, logFiles.get(5).getFileName());
  }

  public static String makeOldLogFileName(String fileId, String logFileExtension, String baseCommitTime, int version) {
    return "." + String.format("%s_%s%s.%d", fileId, baseCommitTime, logFileExtension, version);
  }

  @Test
  public void testDeleteOlderRollbackFiles() throws Exception {
    String[] instantTimes = new String[]{"20160501010101", "20160501020101", "20160501030101", "20160501040101",
        "20160502020601", "20160502030601", "20160502040601", "20160502050601", "20160506030611",
        "20160506040611", "20160506050611", "20160506060611"};
    List<HoodieInstant> hoodieInstants = new ArrayList<>();
    // create rollback files
    for (String instantTime : instantTimes) {
      Files.createFile(Paths.get(basePath,
          HoodieTableMetaClient.METAFOLDER_NAME,
          instantTime + HoodieTimeline.ROLLBACK_EXTENSION));
      hoodieInstants.add(new HoodieInstant(false, HoodieTimeline.ROLLBACK_ACTION, instantTime));
    }

    String metaPath = Paths.get(basePath, ".hoodie").toString();
    FSUtils.deleteOlderRollbackMetaFiles(FSUtils.getFs(basePath, new Configuration()),
        metaPath, hoodieInstants.stream());
    File[] rollbackFiles = new File(metaPath).listFiles((dir, name)
        -> name.contains(HoodieTimeline.ROLLBACK_EXTENSION));
    assertNotNull(rollbackFiles);
    assertEquals(rollbackFiles.length, minRollbackToKeep);
  }

  @Test
  public void testDeleteOlderCleanMetaFiles() throws Exception {
    String[] instantTimes = new String[]{"20160501010101", "20160501020101", "20160501030101", "20160501040101",
        "20160502020601", "20160502030601", "20160502040601", "20160502050601", "20160506030611",
        "20160506040611", "20160506050611", "20160506060611"};
    List<HoodieInstant> hoodieInstants = new ArrayList<>();
    // create rollback files
    for (String instantTime : instantTimes) {
      Files.createFile(Paths.get(basePath,
          HoodieTableMetaClient.METAFOLDER_NAME,
          instantTime + HoodieTimeline.CLEAN_EXTENSION));
      hoodieInstants.add(new HoodieInstant(false, HoodieTimeline.CLEAN_ACTION, instantTime));
    }
    String metaPath = Paths.get(basePath, ".hoodie").toString();
    FSUtils.deleteOlderCleanMetaFiles(FSUtils.getFs(basePath, new Configuration()),
        metaPath, hoodieInstants.stream());
    File[] cleanFiles = new File(metaPath).listFiles((dir, name)
        -> name.contains(HoodieTimeline.CLEAN_EXTENSION));
    assertNotNull(cleanFiles);
    assertEquals(cleanFiles.length, minCleanToKeep);
  }

  @Test
  public void testFileNameRelatedFunctions() throws Exception {
    String instantTime = "20160501010101";
    String partitionStr = "2016/05/01";
    String writeToken = "456";
    String fileId = "Id123";
    int version = 1;
    final String LOG_STR = "log";
    final String LOG_EXTENTION = "." + LOG_STR;

    // base file name
    String dataFileName = FSUtils.makeBaseFileName(instantTime, writeToken, fileId);
    assertEquals(instantTime, FSUtils.getCommitTime(dataFileName));
    assertEquals(fileId, FSUtils.getFileId(dataFileName));

    String logFileName = FSUtils.makeLogFileName(fileId, LOG_EXTENTION, instantTime, version, writeToken);
    assertTrue(FSUtils.isLogFile(new Path(logFileName)));
    assertEquals(instantTime, FSUtils.getBaseCommitTimeFromLogPath(new Path(logFileName)));
    assertEquals(fileId, FSUtils.getFileIdFromLogPath(new Path(logFileName)));
    assertEquals(version, FSUtils.getFileVersionFromLog(new Path(logFileName)));
    assertEquals(LOG_STR, FSUtils.getFileExtensionFromLog(new Path(logFileName)));

    // create three versions of log file
    java.nio.file.Path partitionPath = Paths.get(basePath, partitionStr);
    Files.createDirectories(partitionPath);
    String log1 = FSUtils.makeLogFileName(fileId, LOG_EXTENTION, instantTime, 1, writeToken);
    Files.createFile(partitionPath.resolve(log1));
    String log2 = FSUtils.makeLogFileName(fileId, LOG_EXTENTION, instantTime, 2, writeToken);
    Files.createFile(partitionPath.resolve(log2));
    String log3 = FSUtils.makeLogFileName(fileId, LOG_EXTENTION, instantTime, 3, writeToken);
    Files.createFile(partitionPath.resolve(log3));

    assertEquals(3, (int) FSUtils.getLatestLogVersion(FSUtils.getFs(basePath, new Configuration()),
        new Path(partitionPath.toString()), fileId, LOG_EXTENTION, instantTime).get().getLeft());
    assertEquals(4, FSUtils.computeNextLogVersion(FSUtils.getFs(basePath, new Configuration()),
        new Path(partitionPath.toString()), fileId, LOG_EXTENTION, instantTime));
  }
}
