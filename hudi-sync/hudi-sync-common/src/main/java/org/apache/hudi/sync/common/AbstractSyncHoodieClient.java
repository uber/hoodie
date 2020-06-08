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

package org.apache.hudi.sync.common;

import org.apache.hudi.common.util.Option;
import org.apache.parquet.schema.MessageType;
import java.util.List;

public abstract class AbstractSyncHoodieClient {

  public abstract void createTable(String tableName, MessageType storageSchema,
                                   String inputFormatClass, String outputFormatClass, String serdeClass);

  public abstract boolean doesTableExist(String tableName);

  public abstract Option<String> getLastCommitTimeSynced(String tableName);

  public abstract void updateLastCommitTimeSynced(String tableName);

  public abstract void addPartitionsToTable(String tableName, List<String> partitionsToAdd);

  public abstract void updatePartitionsToTable(String tableName, List<String> changedPartitions);

}
