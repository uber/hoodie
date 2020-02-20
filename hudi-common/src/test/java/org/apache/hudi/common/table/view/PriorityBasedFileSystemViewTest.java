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

package org.apache.hudi.common.table.view;

import junit.framework.TestCase;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.SyncableFileSystemView;
import org.apache.hudi.common.table.string.MockHoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class PriorityBasedFileSystemViewTest extends TestCase {

  @Mock
  private SyncableFileSystemView primary;

  @Mock
  private SyncableFileSystemView secondary;

  @InjectMocks
  private PriorityBasedFileSystemView fsView;

  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  private Stream<HoodieBaseFile> testBaseFileStream;
  private Stream<FileSlice> testFileSliceStream;

  @Before
  public void setUp() {
    fsView = new PriorityBasedFileSystemView(primary, secondary);
    testBaseFileStream = Collections.singleton(new HoodieBaseFile("test")).stream();
    testFileSliceStream = Collections
        .singleton(new FileSlice("2020-01-01", "20:20", "file0001.parquet")).stream();
  }

  private void resetMocks() {
    reset(primary, secondary);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetLatestBaseFiles() {
    Stream<HoodieBaseFile> actual;
    Stream<HoodieBaseFile> expected = testBaseFileStream;

    when(primary.getLatestBaseFiles()).thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFiles();
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestBaseFiles()).thenThrow(new RuntimeException());
    when(secondary.getLatestBaseFiles()).thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFiles();
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestBaseFiles()).thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFiles();
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestBaseFiles()).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestBaseFiles();
  }

  @Test
  public void testGetLatestBaseFilesWithPartitionPath() {
    Stream<HoodieBaseFile> actual;
    Stream<HoodieBaseFile> expected = testBaseFileStream;
    String partitionPath = "/table2";

    when(primary.getLatestBaseFiles(partitionPath)).thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFiles(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestBaseFiles(partitionPath)).thenThrow(new RuntimeException());
    when(secondary.getLatestBaseFiles(partitionPath)).thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFiles(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestBaseFiles(partitionPath)).thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFiles(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestBaseFiles(partitionPath)).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestBaseFiles(partitionPath);
  }

  @Test
  public void testGetLatestBaseFilesBeforeOrOn() {
    Stream<HoodieBaseFile> actual;
    Stream<HoodieBaseFile> expected = testBaseFileStream;
    String partitionPath = "/table2";
    String maxCommitTime = "2010-10-10";

    when(primary.getLatestBaseFilesBeforeOrOn(partitionPath, maxCommitTime))
        .thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFilesBeforeOrOn(partitionPath, maxCommitTime);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestBaseFilesBeforeOrOn(partitionPath, maxCommitTime))
        .thenThrow(new RuntimeException());
    when(secondary.getLatestBaseFilesBeforeOrOn(partitionPath, maxCommitTime))
        .thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFilesBeforeOrOn(partitionPath, maxCommitTime);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestBaseFilesBeforeOrOn(partitionPath, maxCommitTime))
        .thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFilesBeforeOrOn(partitionPath, maxCommitTime);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestBaseFilesBeforeOrOn(partitionPath, maxCommitTime))
        .thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestBaseFilesBeforeOrOn(partitionPath, maxCommitTime);
  }

  @Test
  public void testGetLatestBaseFile() {
    Option<HoodieBaseFile> actual;
    Option<HoodieBaseFile> expected = Option.of(new HoodieBaseFile("test.file"));
    String partitionPath = "/table2";
    String fileID = "file.123";

    when(primary.getLatestBaseFile(partitionPath, fileID)).thenReturn(expected);
    actual = fsView.getLatestBaseFile(partitionPath, fileID);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestBaseFile(partitionPath, fileID)).thenThrow(new RuntimeException());
    when(secondary.getLatestBaseFile(partitionPath, fileID)).thenReturn(expected);
    actual = fsView.getLatestBaseFile(partitionPath, fileID);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestBaseFile(partitionPath, fileID)).thenReturn(expected);
    actual = fsView.getLatestBaseFile(partitionPath, fileID);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestBaseFile(partitionPath, fileID)).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestBaseFile(partitionPath, fileID);
  }

  @Test
  public void testGetBaseFileOn() {
    Option<HoodieBaseFile> actual;
    Option<HoodieBaseFile> expected = Option.of(new HoodieBaseFile("test.file"));
    String partitionPath = "/table2";
    String instantTime = "2020-01-01";
    String fileID = "file.123";

    when(primary.getBaseFileOn(partitionPath, instantTime, fileID)).thenReturn(expected);
    actual = fsView.getBaseFileOn(partitionPath, instantTime, fileID);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getBaseFileOn(partitionPath, instantTime, fileID))
        .thenThrow(new RuntimeException());
    when(secondary.getBaseFileOn(partitionPath, instantTime, fileID)).thenReturn(expected);
    actual = fsView.getBaseFileOn(partitionPath, instantTime, fileID);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getBaseFileOn(partitionPath, instantTime, fileID)).thenReturn(expected);
    actual = fsView.getBaseFileOn(partitionPath, instantTime, fileID);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getBaseFileOn(partitionPath, instantTime, fileID))
        .thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getBaseFileOn(partitionPath, instantTime, fileID);
  }

  @Test
  public void testGetLatestBaseFilesInRange() {
    Stream<HoodieBaseFile> actual;
    Stream<HoodieBaseFile> expected = testBaseFileStream;
    List<String> commitsToReturn = Collections.singletonList("/table2");

    when(primary.getLatestBaseFilesInRange(commitsToReturn)).thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFilesInRange(commitsToReturn);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestBaseFilesInRange(commitsToReturn)).thenThrow(new RuntimeException());
    when(secondary.getLatestBaseFilesInRange(commitsToReturn)).thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFilesInRange(commitsToReturn);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestBaseFilesInRange(commitsToReturn)).thenReturn(testBaseFileStream);
    actual = fsView.getLatestBaseFilesInRange(commitsToReturn);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestBaseFilesInRange(commitsToReturn)).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestBaseFilesInRange(commitsToReturn);
  }

  @Test
  public void testGetAllBaseFiles() {
    Stream<HoodieBaseFile> actual;
    Stream<HoodieBaseFile> expected = testBaseFileStream;
    String partitionPath = "/table2";

    when(primary.getAllBaseFiles(partitionPath)).thenReturn(testBaseFileStream);
    actual = fsView.getAllBaseFiles(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getAllBaseFiles(partitionPath)).thenThrow(new RuntimeException());
    when(secondary.getAllBaseFiles(partitionPath)).thenReturn(testBaseFileStream);
    actual = fsView.getAllBaseFiles(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getAllBaseFiles(partitionPath)).thenReturn(testBaseFileStream);
    actual = fsView.getAllBaseFiles(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getAllBaseFiles(partitionPath)).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getAllBaseFiles(partitionPath);
  }

  @Test
  public void testGetLatestFileSlices() {
    Stream<FileSlice> actual;
    Stream<FileSlice> expected = testFileSliceStream;
    String partitionPath = "/table2";

    when(primary.getLatestFileSlices(partitionPath)).thenReturn(testFileSliceStream);
    actual = fsView.getLatestFileSlices(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestFileSlices(partitionPath)).thenThrow(new RuntimeException());
    when(secondary.getLatestFileSlices(partitionPath)).thenReturn(testFileSliceStream);
    actual = fsView.getLatestFileSlices(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestFileSlices(partitionPath)).thenReturn(testFileSliceStream);
    actual = fsView.getLatestFileSlices(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestFileSlices(partitionPath)).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestFileSlices(partitionPath);
  }

  @Test
  public void testGetLatestUnCompactedFileSlices() {
    Stream<FileSlice> actual;
    Stream<FileSlice> expected = testFileSliceStream;
    String partitionPath = "/table2";

    when(primary.getLatestUnCompactedFileSlices(partitionPath)).thenReturn(testFileSliceStream);
    actual = fsView.getLatestUnCompactedFileSlices(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestUnCompactedFileSlices(partitionPath)).thenThrow(new RuntimeException());
    when(secondary.getLatestUnCompactedFileSlices(partitionPath)).thenReturn(testFileSliceStream);
    actual = fsView.getLatestUnCompactedFileSlices(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestUnCompactedFileSlices(partitionPath)).thenReturn(testFileSliceStream);
    actual = fsView.getLatestUnCompactedFileSlices(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestUnCompactedFileSlices(partitionPath)).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestUnCompactedFileSlices(partitionPath);
  }

  @Test
  public void testGetLatestFileSlicesBeforeOrOn() {
    Stream<FileSlice> actual;
    Stream<FileSlice> expected = testFileSliceStream;
    String partitionPath = "/table2";
    String maxCommitTime = "2020-01-01";

    when(primary.getLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime, false))
        .thenReturn(testFileSliceStream);
    actual = fsView.getLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime, false);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime, false))
        .thenThrow(new RuntimeException());
    when(secondary.getLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime, false))
        .thenReturn(testFileSliceStream);
    actual = fsView.getLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime, false);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime, false))
        .thenReturn(testFileSliceStream);
    actual = fsView.getLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime, false);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime, false))
        .thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime, false);
  }

  @Test
  public void testGetLatestMergedFileSlicesBeforeOrOn() {
    Stream<FileSlice> actual;
    Stream<FileSlice> expected = testFileSliceStream;
    String partitionPath = "/table2";
    String maxInstantTime = "2020-01-01";

    when(primary.getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime))
        .thenReturn(testFileSliceStream);
    actual = fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime))
        .thenThrow(new RuntimeException());
    when(secondary.getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime))
        .thenReturn(testFileSliceStream);
    actual = fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime))
        .thenReturn(testFileSliceStream);
    actual = fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime))
        .thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, maxInstantTime);
  }

  @Test
  public void testGetLatestFileSliceInRange() {
    Stream<FileSlice> actual;
    Stream<FileSlice> expected = testFileSliceStream;
    List<String> commitsToReturn = Collections.singletonList("/table2");

    when(primary.getLatestFileSliceInRange(commitsToReturn)).thenReturn(testFileSliceStream);
    actual = fsView.getLatestFileSliceInRange(commitsToReturn);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestFileSliceInRange(commitsToReturn)).thenThrow(new RuntimeException());
    when(secondary.getLatestFileSliceInRange(commitsToReturn)).thenReturn(testFileSliceStream);
    actual = fsView.getLatestFileSliceInRange(commitsToReturn);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestFileSliceInRange(commitsToReturn)).thenReturn(testFileSliceStream);
    actual = fsView.getLatestFileSliceInRange(commitsToReturn);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestFileSliceInRange(commitsToReturn)).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestFileSliceInRange(commitsToReturn);
  }

  @Test
  public void testGetAllFileSlices() {
    Stream<FileSlice> actual;
    Stream<FileSlice> expected = testFileSliceStream;
    String partitionPath = "/table2";

    when(primary.getAllFileSlices(partitionPath)).thenReturn(testFileSliceStream);
    actual = fsView.getAllFileSlices(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getAllFileSlices(partitionPath)).thenThrow(new RuntimeException());
    when(secondary.getAllFileSlices(partitionPath)).thenReturn(testFileSliceStream);
    actual = fsView.getAllFileSlices(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getAllFileSlices(partitionPath)).thenReturn(testFileSliceStream);
    actual = fsView.getAllFileSlices(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getAllFileSlices(partitionPath)).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getAllFileSlices(partitionPath);
  }

  @Test
  public void testGetAllFileGroups() {
    Stream<HoodieFileGroup> actual;
    String partitionPath = "/table2";
    Stream<HoodieFileGroup> expected = Collections.singleton(
        new HoodieFileGroup(partitionPath, "id1",
            new MockHoodieTimeline(Stream.empty(), Stream.empty()))).stream();

    when(primary.getAllFileGroups(partitionPath)).thenReturn(expected);
    actual = fsView.getAllFileGroups(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getAllFileGroups(partitionPath)).thenThrow(new RuntimeException());
    when(secondary.getAllFileGroups(partitionPath)).thenReturn(expected);
    actual = fsView.getAllFileGroups(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getAllFileGroups(partitionPath)).thenReturn(expected);
    actual = fsView.getAllFileGroups(partitionPath);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getAllFileGroups(partitionPath)).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getAllFileGroups(partitionPath);
  }

  @Test
  public void testGetPendingCompactionOperations() {
    Stream<Pair<String, CompactionOperation>> actual;
    Stream<Pair<String, CompactionOperation>> expected = Collections.singleton(
        (Pair<String, CompactionOperation>) new ImmutablePair<>("test", new CompactionOperation()))
        .stream();

    when(primary.getPendingCompactionOperations()).thenReturn(expected);
    actual = fsView.getPendingCompactionOperations();
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getPendingCompactionOperations()).thenThrow(new RuntimeException());
    when(secondary.getPendingCompactionOperations()).thenReturn(expected);
    actual = fsView.getPendingCompactionOperations();
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getPendingCompactionOperations()).thenReturn(expected);
    actual = fsView.getPendingCompactionOperations();
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getPendingCompactionOperations()).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getPendingCompactionOperations();
  }

  @Test
  public void testClose() {
    fsView.close();
    verify(primary, times(1)).close();
    verify(secondary, times(1)).close();
  }

  @Test
  public void testReset() {
    fsView.reset();
    verify(primary, times(1)).reset();
    verify(secondary, times(1)).reset();
  }

  @Test
  public void testGetLastInstant() {
    Option<HoodieInstant> actual;
    Option<HoodieInstant> expected = Option.of(new HoodieInstant(true, "", ""));

    when(primary.getLastInstant()).thenReturn(expected);
    actual = fsView.getLastInstant();
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLastInstant()).thenThrow(new RuntimeException());
    when(secondary.getLastInstant()).thenReturn(expected);
    actual = fsView.getLastInstant();
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLastInstant()).thenReturn(expected);
    actual = fsView.getLastInstant();
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLastInstant()).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLastInstant();
  }

  @Test
  public void testGetTimeline() {
    HoodieTimeline actual;
    HoodieTimeline expected = new MockHoodieTimeline(Stream.empty(), Stream.empty());

    when(primary.getTimeline()).thenReturn(expected);
    actual = fsView.getTimeline();
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getTimeline()).thenThrow(new RuntimeException());
    when(secondary.getTimeline()).thenReturn(expected);
    actual = fsView.getTimeline();
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getTimeline()).thenReturn(expected);
    actual = fsView.getTimeline();
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getTimeline()).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getTimeline();
  }

  @Test
  public void testSync() {
    fsView.sync();
    verify(primary, times(1)).reset();
    verify(secondary, times(1)).reset();
  }

  @Test
  public void testGetLatestFileSlice() {
    Option<FileSlice> actual;
    Option<FileSlice> expected = Option.fromJavaOptional(testFileSliceStream.findFirst());
    String partitionPath = "/table2";
    String fileID = "file.123";

    when(primary.getLatestFileSlice(partitionPath, fileID)).thenReturn(expected);
    actual = fsView.getLatestFileSlice(partitionPath, fileID);
    assertEquals(expected, actual);

    resetMocks();
    when(primary.getLatestFileSlice(partitionPath, fileID)).thenThrow(new RuntimeException());
    when(secondary.getLatestFileSlice(partitionPath, fileID)).thenReturn(expected);
    actual = fsView.getLatestFileSlice(partitionPath, fileID);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestFileSlice(partitionPath, fileID)).thenReturn(expected);
    actual = fsView.getLatestFileSlice(partitionPath, fileID);
    assertEquals(expected, actual);

    resetMocks();
    when(secondary.getLatestFileSlice(partitionPath, fileID)).thenThrow(new RuntimeException());
    thrown.expect(RuntimeException.class);
    fsView.getLatestFileSlice(partitionPath, fileID);
  }

  @Test
  public void testGetPreferredView() {
    assertEquals(primary, fsView.getPreferredView());
  }

  @Test
  public void testGetSecondaryView() {
    assertEquals(secondary, fsView.getSecondaryView());
  }
}