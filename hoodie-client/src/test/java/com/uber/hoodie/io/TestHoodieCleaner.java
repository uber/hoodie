/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.uber.hoodie.common.model.HoodieCleaningPolicy;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.table.HoodieTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests around Cleaning logic in Hoodie
 */
@RunWith(Parameterized.class)
public class TestHoodieCleaner {

    private String basePath = null;
    private String[] partitionPaths = {"2016/01/01", "2016/02/02"};
    private HoodieTableMetaClient metaClient;
    private HoodieTableType tableType;

    @Parameterized.Parameters(name = "HoodieTableTypes")
    public static Collection<HoodieTableType[]> data() {
        return Arrays.asList(new HoodieTableType[][]{
                {HoodieTableType.COPY_ON_WRITE},
                {HoodieTableType.MERGE_ON_READ}
        });
    }

    public TestHoodieCleaner(HoodieTableType tType) {
        this.tableType = tType;
    }

    @Before
    public void init() throws Exception {
        this.metaClient = HoodieTestUtils.initOnTemp();
        this.basePath = metaClient.getBasePath();
    }

    @Test
    public void testKeepLatestFileVersions() throws IOException {
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
                .retainFileVersions(1).build()).build();

        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            // make 1 commit, with 1 file per partition
            HoodieTestUtils.createCommitFiles(basePath, "000");

            String file1P0C0 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "000");
            String file1P1C0 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[1], "000");

            HoodieTableMetaClient metadata = new HoodieTableMetaClient(FSUtils.getFs(), basePath);
            HoodieTable table = HoodieTable.getHoodieTable(metadata, config);

            HoodieCleaner cleaner = new HoodieCleaner(table, config);
            assertEquals("Must not clean any files", 0, cleaner.clean(partitionPaths[0]).getSuccessDeleteFiles().size());
            assertEquals("Must not clean any files", 0, cleaner.clean(partitionPaths[1]).getSuccessDeleteFiles().size());
            assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));
            assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "000", file1P1C0));

            // make next commit, with 1 insert & 1 update per partition
            HoodieTestUtils.createCommitFiles(basePath, "001");

            String file2P0C1 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "001"); // insert
            String file2P1C1 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[1], "001"); // insert
            HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "001", file1P0C0); // update
            HoodieTestUtils.createDataFile(basePath, partitionPaths[1], "001", file1P1C0); // update
            metadata = new HoodieTableMetaClient(FSUtils.getFs(), basePath);
            table = HoodieTable.getHoodieTable(metadata, config);

            cleaner = new HoodieCleaner(table, config);
            assertEquals("Must clean 1 file", 1, cleaner.clean(partitionPaths[0]).getSuccessDeleteFiles().size());
            assertEquals("Must clean 1 file", 1, cleaner.clean(partitionPaths[1]).getSuccessDeleteFiles().size());
            assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file2P0C1));
            assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "001", file2P1C1));
            assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));
            assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "000", file1P1C0));

            // make next commit, with 2 updates to existing files, and 1 insert
            HoodieTestUtils.createCommitFiles(basePath, "002");

            HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "002", file1P0C0); // update
            HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "002", file2P0C1); // update
            String file3P0C2 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "002");
            metadata = new HoodieTableMetaClient(FSUtils.getFs(), basePath);
            table = HoodieTable.getHoodieTable(metadata, config);

            cleaner = new HoodieCleaner(table, config);
            assertEquals("Must clean two files", 2, cleaner.clean(partitionPaths[0]).getSuccessDeleteFiles().size());
            assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file1P0C0));
            assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file2P0C1));
            assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "002", file3P0C2));

            // No cleaning on partially written file, with no commit.
            HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "003", file3P0C2); // update
            assertEquals("Must not clean any files", 0, cleaner.clean(partitionPaths[0]).getSuccessDeleteFiles().size());
            assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "002", file3P0C2));
        } else {
            this.metaClient = HoodieTestUtils.initTableType(basePath, HoodieTableType.MERGE_ON_READ);
            this.basePath = metaClient.getBasePath();
            String fileID = UUID.randomUUID().toString();

            // Make 2 files, one base file and one log file associated with base file
            String file1P0 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "000");
            String file2P0L0 = HoodieTestUtils.createNewLogFile(basePath, partitionPaths[0], "000", file1P0);
            // make 1 compaction commit
            HoodieTestUtils.createCompactionCommitFiles(basePath, "000");

            // Make 2 files, one base file and one log file associated with base file
            HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "001", file1P0);
            file2P0L0 = HoodieTestUtils.createNewLogFile(basePath, partitionPaths[0], "001", file2P0L0);
            // make 1 compaction commit
            HoodieTestUtils.createCompactionCommitFiles(basePath, "001");

            HoodieTableMetaClient metadata = new HoodieTableMetaClient(FSUtils.getFs(), basePath);
            HoodieTable table = HoodieTable.getHoodieTable(metadata, config);

            HoodieCleaner cleaner = new HoodieCleaner(table, config);
            assertEquals("Must clean files" , 2, cleaner.clean(partitionPaths[0]).getSuccessDeleteFiles().size());
            assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0));
            assertFalse(HoodieTestUtils.doesLogFileExist(basePath, partitionPaths[0], "000", file2P0L0));
        }
    }


    @Test
    public void testKeepLatestCommits() throws IOException {

        if(tableType == HoodieTableType.MERGE_ON_READ) {
            return;
        }

        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
                .retainCommits(2).build()).build();


        // make 1 commit, with 1 file per partition
        HoodieTestUtils.createCommitFiles(basePath, "000");

        String file1P0C0 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "000");
        String file1P1C0 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[1], "000");

        HoodieTableMetaClient metadata = new HoodieTableMetaClient(FSUtils.getFs(), basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metadata, config);

        HoodieCleaner cleaner = new HoodieCleaner(table, config);
        assertEquals("Must not clean any files" , 0, cleaner.clean(partitionPaths[0]).getSuccessDeleteFiles().size());
        assertEquals("Must not clean any files" , 0, cleaner.clean(partitionPaths[1]).getSuccessDeleteFiles().size());
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "000", file1P1C0));

        // make next commit, with 1 insert & 1 update per partition
        HoodieTestUtils.createCommitFiles(basePath, "001");

        String file2P0C1 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "001"); // insert
        String file2P1C1 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[1], "001"); // insert
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "001", file1P0C0); // update
        HoodieTestUtils.createDataFile(basePath, partitionPaths[1], "001", file1P1C0); // update
        metadata = new HoodieTableMetaClient(FSUtils.getFs(), basePath);
        table = HoodieTable.getHoodieTable(metadata, config);

        cleaner = new HoodieCleaner(table, config);
        assertEquals("Must not clean any files" , 0, cleaner.clean(partitionPaths[0]).getSuccessDeleteFiles().size());
        assertEquals("Must not clean any files" , 0, cleaner.clean(partitionPaths[1]).getSuccessDeleteFiles().size());
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file2P0C1));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "001", file2P1C1));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[1], "000", file1P1C0));

        // make next commit, with 2 updates to existing files, and 1 insert
        HoodieTestUtils.createCommitFiles(basePath, "002");

        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "002", file1P0C0); // update
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "002", file2P0C1); // update
        String file3P0C2 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "002");
        metadata = new HoodieTableMetaClient(FSUtils.getFs(), basePath);
        table = HoodieTable.getHoodieTable(metadata, config);

        cleaner = new HoodieCleaner(table, config);
        assertEquals(
            "Must not clean any file. We have to keep 1 version before the latest commit time to keep",
            0, cleaner.clean(partitionPaths[0]).getSuccessDeleteFiles().size());

        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));

        // make next commit, with 2 updates to existing files, and 1 insert
        HoodieTestUtils.createCommitFiles(basePath, "003");

        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "003", file1P0C0); // update
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "003", file2P0C1); // update
        String file4P0C3 = HoodieTestUtils.createNewDataFile(basePath, partitionPaths[0], "003");
        metadata = new HoodieTableMetaClient(FSUtils.getFs(), basePath);
        table = HoodieTable.getHoodieTable(metadata, config);

        cleaner = new HoodieCleaner(table, config);
        assertEquals(
            "Must not clean one old file", 1, cleaner.clean(partitionPaths[0]).getSuccessDeleteFiles().size());

        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "000", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "002", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file2P0C1));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "002", file2P0C1));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "002", file3P0C2));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "003", file4P0C3));

        // No cleaning on partially written file, with no commit.
        HoodieTestUtils.createDataFile(basePath, partitionPaths[0], "004", file3P0C2); // update
        assertEquals("Must not clean any files" , 0, cleaner.clean(partitionPaths[0]).getSuccessDeleteFiles().size());
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file1P0C0));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, partitionPaths[0], "001", file2P0C1));
    }
}
