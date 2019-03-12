/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.bench.reader;

import com.uber.hoodie.AvroConversionUtils;
import java.util.List;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

/**
 * Helper class to read avro files and generate a RDD of {@link GenericRecord}
 */
public class SparkBasedReader {

  public static final String SPARK_AVRO_FORMAT = "com.databricks.spark.avro";
  public static final String SPARK_PARQUET_FORMAT = "com.databricks.spark.parquet";
  private static final String AVRO_SCHEMA_OPTION_KEY = "avroSchema";
  private static final String DEFAULT_STRUCT_NAME = "test.struct";
  private static final String DEFAULT_NAMESPACE_NAME = "test.namespace";

  // Spark anyways globs the path and gets all the paths in memory so take the List<filePaths> as an argument.
  // https://github.com/apache/spark/.../org/apache/spark/sql/execution/datasources/DataSource.scala#L251
  public static JavaRDD<GenericRecord> readAvro(SparkSession sparkSession, String schemaStr, List<String> listOfPaths,
      Optional<String> structName, Optional<String> nameSpace) {

    Dataset<Row> dataSet = sparkSession.read()
        .format(SPARK_AVRO_FORMAT)
        .option(AVRO_SCHEMA_OPTION_KEY, schemaStr)
        .load(JavaConverters.asScalaIteratorConverter(listOfPaths.iterator()).asScala().toSeq());

    return AvroConversionUtils
        .createRdd(dataSet.toDF(), structName.orElse(DEFAULT_STRUCT_NAME), nameSpace.orElse(DEFAULT_NAMESPACE_NAME))
        .toJavaRDD();
  }

  public static JavaRDD<GenericRecord> readParquet(SparkSession sparkSession, List<String>
      listOfPaths, Optional<String> structName, Optional<String> nameSpace) {

    Dataset<Row> dataSet = sparkSession.read()
        .parquet((JavaConverters.asScalaIteratorConverter(listOfPaths.iterator()).asScala().toSeq()));

    return AvroConversionUtils
        .createRdd(dataSet.toDF(), structName.orElse(DEFAULT_STRUCT_NAME), nameSpace.orElse(DEFAULT_NAMESPACE_NAME))
        .toJavaRDD();
  }

}
