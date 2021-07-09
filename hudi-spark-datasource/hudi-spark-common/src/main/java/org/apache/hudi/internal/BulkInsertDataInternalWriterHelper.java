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

package org.apache.hudi.internal;

import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieAppendOnlyRowCreateHandle;
import org.apache.hudi.io.HoodieRowCreateHandle;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Helper class for HoodieBulkInsertDataInternalWriter used by Spark datasource v2.
 */
public class BulkInsertDataInternalWriterHelper {

  private static final Logger LOG = LogManager.getLogger(BulkInsertDataInternalWriterHelper.class);

  private final String instantTime;
  private final int taskPartitionId;
  private final long taskId;
  private final long taskEpochId;
  private final HoodieTable hoodieTable;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final Boolean arePartitionRecordsSorted;
  private final List<HoodieInternalWriteStatus> writeStatusList = new ArrayList<>();
  private final boolean populateMetaColumns;
  private HoodieRowCreateHandle handle;
  private String lastKnownPartitionPath = null;
  private String fileIdPrefix;
  private int numFilesWritten = 0;
  private Map<String, HoodieRowCreateHandle> handles = new HashMap<>();
  private final BuiltinKeyGenerator keyGenerator;
  private final boolean simpleKeyGen;
  private int simplePartitionFieldIndex = -1;
  private DataType simplePartitionFieldDataType;

  public BulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                            String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType,
                                            boolean populateMetaColumns, boolean arePartitionRecordsSorted,
                                            BuiltinKeyGenerator keyGenerator) {
    this.hoodieTable = hoodieTable;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.taskPartitionId = taskPartitionId;
    this.taskId = taskId;
    this.taskEpochId = taskEpochId;
    this.structType = structType;
    this.populateMetaColumns = populateMetaColumns;
    this.arePartitionRecordsSorted = arePartitionRecordsSorted;
    this.fileIdPrefix = UUID.randomUUID().toString();
    this.keyGenerator = keyGenerator;
    if (!populateMetaColumns && keyGenerator instanceof SimpleKeyGenerator) {
      simpleKeyGen = true;
      simplePartitionFieldIndex = (Integer) structType.getFieldIndex((keyGenerator).getPartitionPathFields().get(0)).get();
      simplePartitionFieldDataType = structType.fields()[simplePartitionFieldIndex].dataType();
    } else {
      simpleKeyGen = false;
    }
  }

  public void write(InternalRow record) throws IOException {
    try {
      String partitionPath = null;
      if (populateMetaColumns) { // usual path where meta columns are pre populated in prep step.
        partitionPath = record.getUTF8String(
            HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).toString();
      } else { // if meta columns are disabled.
        if (keyGenerator == null) { // NoPartitionerKeyGen
          partitionPath = "";
        } else if (simpleKeyGen) { // SimpleKeyGen
          partitionPath = (record.get(simplePartitionFieldIndex, simplePartitionFieldDataType)).toString();
        } else {
          // only BuiltIn key gens are supported if meta columns are disabled.
          partitionPath = keyGenerator.getPartitionPath(record, structType);
        }
      }

      if ((lastKnownPartitionPath == null) || !lastKnownPartitionPath.equals(partitionPath) || !handle.canWrite()) {
        LOG.info("Creating new file for partition path " + partitionPath);
        handle = getRowCreateHandle(partitionPath);
        lastKnownPartitionPath = partitionPath;
      }
      handle.write(record);
    } catch (Throwable t) {
      LOG.error("Global error thrown while trying to write records in HoodieRowCreateHandle ", t);
      throw t;
    }
  }

  public List<HoodieInternalWriteStatus> getWriteStatuses() throws IOException {
    close();
    return writeStatusList;
  }

  public void abort() {
  }

  private HoodieRowCreateHandle getRowCreateHandle(String partitionPath) throws IOException {
    if (!handles.containsKey(partitionPath)) { // if there is no handle corresponding to the partition path
      // if records are sorted, we can close all existing handles
      if (arePartitionRecordsSorted) {
        close();
      }
      handles.put(partitionPath, populateMetaColumns ? new HoodieRowCreateHandle(hoodieTable, writeConfig, partitionPath, getNextFileId(),
          instantTime, taskPartitionId, taskId, taskEpochId, structType) : new HoodieAppendOnlyRowCreateHandle(hoodieTable, writeConfig, partitionPath, getNextFileId(),
          instantTime, taskPartitionId, taskId, taskEpochId, structType));
    } else if (!handles.get(partitionPath).canWrite()) {
      // even if there is a handle to the partition path, it could have reached its max size threshold. So, we close the handle here and
      // create a new one.
      writeStatusList.add(handles.remove(partitionPath).close());
      handles.put(partitionPath, populateMetaColumns ? new HoodieRowCreateHandle(hoodieTable, writeConfig, partitionPath, getNextFileId(),
          instantTime, taskPartitionId, taskId, taskEpochId, structType) : new HoodieAppendOnlyRowCreateHandle(hoodieTable, writeConfig, partitionPath, getNextFileId(),
          instantTime, taskPartitionId, taskId, taskEpochId, structType));
    }
    return handles.get(partitionPath);
  }

  public void close() throws IOException {
    for (HoodieRowCreateHandle rowCreateHandle : handles.values()) {
      writeStatusList.add(rowCreateHandle.close());
    }
    handles.clear();
    handle = null;
  }

  private String getNextFileId() {
    return String.format("%s-%d", fileIdPrefix, numFilesWritten++);
  }
}
