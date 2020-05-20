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

import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class TestHoodieROTablePathFilter extends HoodieCommonTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @Test
  public void testHoodiePaths() throws IOException {
    // Create a temp folder as the base path
    String basePath = metaClient.getBasePath();

    HoodieTestUtils.createCommitFiles(basePath, "001", "002");
    HoodieTestUtils.createInflightCommitFiles(basePath, "003");
    HoodieTestUtils.createCompactionRequest(metaClient, "004", new ArrayList<>());

    HoodieTestUtils.createBaseFile(basePath, "2017/01/01", "001", "f1");
    HoodieTestUtils.createBaseFile(basePath, "2017/01/01", "001", "f2");
    HoodieTestUtils.createBaseFile(basePath, "2017/01/01", "001", "f3");
    HoodieTestUtils.createBaseFile(basePath, "2017/01/01", "002", "f2");
    HoodieTestUtils.createBaseFile(basePath, "2017/01/01", "003", "f3");

    HoodieROTablePathFilter pathFilter = new HoodieROTablePathFilter();
    Path partitionPath = new Path("file://" + basePath + File.separator + "2017/01/01");
    assertTrue(pathFilter.accept(partitionPath), "Directories should be accepted");

    assertTrue(
        pathFilter.accept(new Path("file:///" + HoodieTestUtils.getBaseFilePath(basePath, "2017/01/01", "001", "f1"))));
    assertFalse(
        pathFilter.accept(new Path("file:///" + HoodieTestUtils.getBaseFilePath(basePath, "2017/01/01", "001", "f2"))));
    assertTrue(
        pathFilter.accept(new Path("file:///" + HoodieTestUtils.getBaseFilePath(basePath, "2017/01/01", "001", "f3"))));
    assertTrue(
        pathFilter.accept(new Path("file:///" + HoodieTestUtils.getBaseFilePath(basePath, "2017/01/01", "002", "f2"))));
    assertFalse(
        pathFilter.accept(new Path("file:///" + HoodieTestUtils.getBaseFilePath(basePath, "2017/01/01", "003", "f3"))));
    assertFalse(pathFilter.accept(new Path("file:///" + HoodieTestUtils.getCommitFilePath(basePath, "001"))));
    assertFalse(pathFilter.accept(new Path("file:///" + HoodieTestUtils.getCommitFilePath(basePath, "002"))));
    assertFalse(pathFilter.accept(new Path("file:///" + HoodieTestUtils.getInflightCommitFilePath(basePath, "003"))));
    assertFalse(
        pathFilter.accept(new Path("file:///" + HoodieTestUtils.getRequestedCompactionFilePath(basePath, "004"))));
    assertFalse(pathFilter.accept(new Path("file:///" + basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/")));
    assertFalse(pathFilter.accept(new Path("file:///" + basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME)));

    assertFalse(
        pathFilter.accept(new Path("file:///" + HoodieTestUtils.getBaseFilePath(basePath, "2017/01/01", "003", "f3"))));

  }

  @Test
  public void testNonHoodiePaths(@TempDir java.nio.file.Path tempDir) throws IOException {
    String basePath = tempDir.toAbsolutePath().toString();
    HoodieROTablePathFilter pathFilter = new HoodieROTablePathFilter();

    String path = basePath + File.separator + "nonhoodiefolder";
    new File(path).mkdirs();
    assertTrue(pathFilter.accept(new Path("file:///" + path)));

    path = basePath + File.separator + "nonhoodiefolder/somefile";
    new File(path).createNewFile();
    assertTrue(pathFilter.accept(new Path("file:///" + path)));
  }
}
