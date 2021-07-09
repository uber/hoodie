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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Properties;

/**
 * Hoodie's Implementation of {@link DataWriter<InternalRow>}. This is used in data source implementation for bulk insert.
 */
public class HoodieBulkInsertDataInternalWriter implements DataWriter<InternalRow> {

  private final BulkInsertDataInternalWriterHelper bulkInsertWriterHelper;

  public HoodieBulkInsertDataInternalWriter(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                            String instantTime, int taskPartitionId, long taskId, long taskEpochId,
                                            StructType structType, boolean populateMetaColumns, boolean arePartitionRecordsSorted) {
    this.bulkInsertWriterHelper = new BulkInsertDataInternalWriterHelper(hoodieTable,
        writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaColumns, arePartitionRecordsSorted,
        populateMetaColumns ? null : getKeyGenerator(writeConfig.getProps()));
  }

  /**
   * Instantiate {@link BuiltinKeyGenerator}.
   *
   * @param properties properties map.
   * @return the key generator thus instantiated.
   */
  private BuiltinKeyGenerator getKeyGenerator(Properties properties) {
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.putAll(properties);
    if (properties.get(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY().key()).equals(NonpartitionedKeyGenerator.class.getName())) {
      return null;
    } else {
      try {
        return (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(typedProperties);
      } catch (ClassCastException cce) {
        throw new HoodieIOException("Only those key gens implementing BuiltInKeyGenerator interface is supported in disabling meta columns path");
      } catch (IOException e) {
        throw new HoodieIOException("Key generator instantiation failed ", e);
      }
    }
  }

  @Override
  public void write(InternalRow record) throws IOException {
    bulkInsertWriterHelper.write(record);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    return new HoodieWriterCommitMessage(bulkInsertWriterHelper.getWriteStatuses());
  }

  @Override
  public void abort() {
    bulkInsertWriterHelper.abort();
  }
}
