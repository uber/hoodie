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

package org.apache.hudi.utilities.deser;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Extending {@link KafkaAvroSchemaDeserializer} as we need to be able to inject reader schema during deserialization.
 */
public class KafkaAvroSchemaDeserializer extends KafkaAvroDeserializer {

  private static final String SCHEMA_PROVIDER_CLASS_PROP = "hoodie.deltastreamer.schemaprovider.class";
  private Schema sourceSchema;

  public KafkaAvroSchemaDeserializer() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    try {
      TypedProperties props = getConvertToTypedProperties(configs);
      String className = props.getString(SCHEMA_PROVIDER_CLASS_PROP);
      SchemaProvider schemaProvider = (SchemaProvider) ReflectionUtils.loadClass(className, props);
      sourceSchema = Objects.requireNonNull(schemaProvider).getSourceSchema();
    } catch (Throwable e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Pretty much copy-paste from the {@link AbstractKafkaAvroDeserializer} except line 87:
   * DatumReader reader = new GenericDatumReader(schema, sourceSchema);
   * <p>
   * We need to inject reader schema during deserialization or later stages of the pipeline break.
   *
   * @param includeSchemaAndVersion
   * @param topic
   * @param isKey
   * @param payload
   * @param readerSchema
   * @return
   * @throws SerializationException
   */
  @Override
  protected Object deserialize(
      boolean includeSchemaAndVersion,
      String topic,
      Boolean isKey,
      byte[] payload,
      Schema readerSchema)
      throws SerializationException {
    return super.deserialize(includeSchemaAndVersion, topic, isKey, payload, sourceSchema);
  }

  private TypedProperties getConvertToTypedProperties(Map<String, ?> configs) {
    TypedProperties typedProperties = new TypedProperties();
    for (Entry<String, ?> entry : configs.entrySet()) {
      typedProperties.put(entry.getKey(), entry.getValue());
    }
    return typedProperties;
  }
}
