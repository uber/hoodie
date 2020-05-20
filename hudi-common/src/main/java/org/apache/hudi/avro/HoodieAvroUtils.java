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

package org.apache.hudi.avro;

import org.apache.avro.JsonProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.SchemaCompatabilityException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.codehaus.jackson.node.NullNode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Helper class to do common stuff across Avro.
 */
public class HoodieAvroUtils {

  private static ThreadLocal<BinaryEncoder> reuseEncoder = ThreadLocal.withInitial(() -> null);

  private static ThreadLocal<BinaryDecoder> reuseDecoder = ThreadLocal.withInitial(() -> null);

  // All metadata fields are optional strings.
  static final Schema METADATA_FIELD_SCHEMA =
      Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));

  private static final Schema RECORD_KEY_SCHEMA = initRecordKeySchema();

  /**
   * Convert a given avro record to bytes.
   */
  public static byte[] avroToBytes(GenericRecord record) throws IOException {
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, reuseEncoder.get());
    reuseEncoder.set(encoder);
    writer.write(record, encoder);
    encoder.flush();
    out.close();
    return out.toByteArray();
  }

  /**
   * Convert a given avro record to json and return the encoded bytes.
   *
   * @param record The GenericRecord to convert
   * @param pretty Whether to pretty-print the json output
   */
  public static byte[] avroToJson(GenericRecord record, boolean pretty) throws IOException {
    DatumWriter<Object> writer = new GenericDatumWriter<>(record.getSchema());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out, pretty);
    writer.write(record, jsonEncoder);
    jsonEncoder.flush();
    return out.toByteArray();
  }

  /**
   * Convert serialized bytes back into avro record.
   */
  public static GenericRecord bytesToAvro(byte[] bytes, Schema schema) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, reuseDecoder.get());
    reuseDecoder.set(decoder);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    return reader.read(null, decoder);
  }

  /**
   * Convert json bytes back into avro record.
   */
  public static GenericRecord jsonBytesToAvro(byte[] bytes, Schema schema) throws IOException {
    ByteArrayInputStream bio = new ByteArrayInputStream(bytes);
    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, bio);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    return reader.read(null, jsonDecoder);
  }

  public static boolean isMetadataField(String fieldName) {
    return HoodieRecord.COMMIT_TIME_METADATA_FIELD.equals(fieldName)
        || HoodieRecord.COMMIT_SEQNO_METADATA_FIELD.equals(fieldName)
        || HoodieRecord.RECORD_KEY_METADATA_FIELD.equals(fieldName)
        || HoodieRecord.PARTITION_PATH_METADATA_FIELD.equals(fieldName)
        || HoodieRecord.FILENAME_METADATA_FIELD.equals(fieldName);
  }

  public static Schema createHoodieWriteSchema(Schema originalSchema) {
    return HoodieAvroUtils.addMetadataFields(originalSchema);
  }

  public static Schema createHoodieWriteSchema(String originalSchema) {
    return createHoodieWriteSchema(new Schema.Parser().parse(originalSchema));
  }

  /**
   * Adds the Hoodie metadata fields to the given schema.
   */
  public static Schema addMetadataFields(Schema schema) {
    List<Schema.Field> parentFields = new ArrayList<>();

    Schema.Field commitTimeField =
        new Schema.Field(HoodieRecord.COMMIT_TIME_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field commitSeqnoField =
        new Schema.Field(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field recordKeyField =
        new Schema.Field(HoodieRecord.RECORD_KEY_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field partitionPathField =
        new Schema.Field(HoodieRecord.PARTITION_PATH_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field fileNameField =
        new Schema.Field(HoodieRecord.FILENAME_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);

    parentFields.add(commitTimeField);
    parentFields.add(commitSeqnoField);
    parentFields.add(recordKeyField);
    parentFields.add(partitionPathField);
    parentFields.add(fileNameField);
    for (Schema.Field field : schema.getFields()) {
      if (!isMetadataField(field.name())) {
        Schema.Field newField = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
        for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
          newField.addProp(prop.getKey(), prop.getValue());
        }
        parentFields.add(newField);
      }
    }

    Schema mergedSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    mergedSchema.setFields(parentFields);
    return mergedSchema;
  }

  public static Schema removeMetadataFields(Schema schema) {
    List<Schema.Field> filteredFields = schema.getFields()
                                              .stream()
                                              .filter(field -> !HoodieRecord.HOODIE_META_COLUMNS.contains(field.name()))
                                              .collect(Collectors.toList());
    Schema filteredSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    filteredSchema.setFields(filteredFields);
    return filteredSchema;
  }

  public static String addMetadataColumnTypes(String hiveColumnTypes) {
    return "string,string,string,string,string," + hiveColumnTypes;
  }

  private static Schema initRecordKeySchema() {
    Schema.Field recordKeyField =
        new Schema.Field(HoodieRecord.RECORD_KEY_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", NullNode.getInstance());
    Schema recordKeySchema = Schema.createRecord("HoodieRecordKey", "", "", false);
    recordKeySchema.setFields(Collections.singletonList(recordKeyField));
    return recordKeySchema;
  }

  public static Schema getRecordKeySchema() {
    return RECORD_KEY_SCHEMA;
  }

  /**
   * Fetch schema for record key and partition path.
   */
  public static Schema getRecordKeyPartitionPathSchema() {
    List<Schema.Field> toBeAddedFields = new ArrayList<>();
    Schema recordSchema = Schema.createRecord("HoodieRecordKey", "", "", false);

    Schema.Field recordKeyField =
        new Schema.Field(HoodieRecord.RECORD_KEY_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", NullNode.getInstance());
    Schema.Field partitionPathField =
        new Schema.Field(HoodieRecord.PARTITION_PATH_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", NullNode.getInstance());

    toBeAddedFields.add(recordKeyField);
    toBeAddedFields.add(partitionPathField);
    recordSchema.setFields(toBeAddedFields);
    return recordSchema;
  }

  public static GenericRecord addHoodieKeyToRecord(GenericRecord record, String recordKey, String partitionPath,
      String fileName) {
    record.put(HoodieRecord.FILENAME_METADATA_FIELD, fileName);
    record.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, partitionPath);
    record.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, recordKey);
    return record;
  }

  /**
   * Add null fields to passed in schema. Caller is responsible for ensuring there is no duplicates. As different query
   * engines have varying constraints regarding treating the case-sensitivity of fields, its best to let caller
   * determine that.
   *
   * @param schema Passed in schema
   * @param newFieldNames Null Field names to be added
   */
  public static Schema appendNullSchemaFields(Schema schema, List<String> newFieldNames) {
    List<Field> newFields = schema.getFields().stream()
        .map(field -> new Field(field.name(), field.schema(), field.doc(), field.defaultValue())).collect(Collectors.toList());
    for (String newField : newFieldNames) {
      newFields.add(new Schema.Field(newField, METADATA_FIELD_SCHEMA, "", NullNode.getInstance()));
    }
    Schema newSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
    newSchema.setFields(newFields);
    return newSchema;
  }

  /**
   * Adds the Hoodie commit metadata into the provided Generic Record.
   */
  public static GenericRecord addCommitMetadataToRecord(GenericRecord record, String instantTime, String commitSeqno) {
    record.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, instantTime);
    record.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, commitSeqno);
    return record;
  }

  /**
   * Given a avro record with a given schema, rewrites it into the new schema while setting fields only from the old
   * schema.
   */
  public static GenericRecord rewriteRecord(GenericRecord record, Schema newSchema) {
    return rewrite(record, getCombinedFieldsToWrite(record.getSchema(), newSchema), newSchema);
  }

  /**
   * Given a avro record with a given schema, rewrites it into the new schema while setting fields only from the new
   * schema.
   */
  public static GenericRecord rewriteRecordWithOnlyNewSchemaFields(GenericRecord record, Schema newSchema) {
    return rewrite(record, new LinkedHashSet<>(newSchema.getFields()), newSchema);
  }

  private static GenericRecord rewrite(GenericRecord record, LinkedHashSet<Field> fieldsToWrite, Schema newSchema) {
    GenericRecord newRecord = new GenericData.Record(newSchema);
    for (Schema.Field f : fieldsToWrite) {
      if (record.get(f.name()) == null) {
        if (f.defaultVal() instanceof JsonProperties.Null) {
          newRecord.put(f.name(), null);
        } else {
          newRecord.put(f.name(), f.defaultVal());
        }
      } else {
        newRecord.put(f.name(), record.get(f.name()));
      }
    }
    if (!GenericData.get().validate(newSchema, newRecord)) {
      throw new SchemaCompatabilityException(
          "Unable to validate the rewritten record " + record + " against schema " + newSchema);
    }
    return newRecord;
  }

  /**
   * Generates a super set of fields from both old and new schema.
   */
  private static LinkedHashSet<Field> getCombinedFieldsToWrite(Schema oldSchema, Schema newSchema) {
    LinkedHashSet<Field> allFields = new LinkedHashSet<>(oldSchema.getFields());
    for (Schema.Field f : newSchema.getFields()) {
      if (!allFields.contains(f) && !isMetadataField(f.name())) {
        allFields.add(f);
      }
    }
    return allFields;
  }

  public static byte[] compress(String text) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      OutputStream out = new DeflaterOutputStream(baos);
      out.write(text.getBytes(StandardCharsets.UTF_8));
      out.close();
    } catch (IOException e) {
      throw new HoodieIOException("IOException while compressing text " + text, e);
    }
    return baos.toByteArray();
  }

  public static String decompress(byte[] bytes) {
    InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buffer = new byte[8192];
      int len;
      while ((len = in.read(buffer)) > 0) {
        baos.write(buffer, 0, len);
      }
      return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new HoodieIOException("IOException while decompressing text", e);
    }
  }
}
