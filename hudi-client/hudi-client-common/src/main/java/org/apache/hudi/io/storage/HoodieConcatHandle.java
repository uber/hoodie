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

package org.apache.hudi.io.storage;

import org.apache.hudi.client.common.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Handle to concatenate new records to old records w/o any merging. If Operation is set to Inserts, and if {{@link HoodieWriteConfig#isRouteInsertsToNewFiles()}}
 * is set, this handle will be used instead of {@link HoodieMergeHandle}
 */
public class HoodieConcatHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieMergeHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieConcatHandle.class);

  public HoodieConcatHandle(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable, Iterator recordItr,
      String partitionPath, String fileId, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier);
  }

  public HoodieConcatHandle(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable, Map keyToNewRecords, String partitionPath, String fileId,
      HoodieBaseFile dataFileToBeMerged, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, keyToNewRecords, partitionPath, fileId, dataFileToBeMerged, taskContextSupplier);
  }

  /**
   * Write old record as is w/o merging with incoming record.
   */
  @Override
  public void write(GenericRecord oldRecord) {
    String key = oldRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    try {
      fileWriter.writeAvro(key, oldRecord);
    } catch (IOException | RuntimeException e) {
      String errMsg = String.format("Failed to write old record into new file for key %s from old file %s to new file %s with writerSchema %s",
          key, getOldFilePath(), newFilePath, writerSchemaWithMetafields.toString(true));
      LOG.debug("Old record is " + oldRecord);
      throw new HoodieUpsertException(errMsg, e);
    }
    recordsWritten++;
  }
}
