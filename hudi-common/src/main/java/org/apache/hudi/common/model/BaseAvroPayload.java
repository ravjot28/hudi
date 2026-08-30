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

package org.apache.hudi.common.model;

import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.Getter;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * Base class for all AVRO record based payloads, that can be ordered based on a field.
 *
 * <p>{@link #getRecord(Schema)} decodes using the record's true writer schema and lets Avro resolve
 * fields into the requested reader schema by name, but only when the requested schema is a pure
 * projection/reorder of the writer schema (every requested field exists in the writer schema by name).
 * When the requested schema instead carries schema-evolution changes (renamed or added columns, i.e.
 * fields the writer schema does not have), it falls back to the legacy positional decode that the
 * merge/read paths have always relied on to carry values across such changes. One exception is
 * intentional: a requested field that the writer schema does not have and that has no default value
 * cannot be resolved either way, so {@code org.apache.avro.AvroTypeException} is thrown rather than
 * silently returning a garbage/positionally-misaligned value for that field -- this loud failure is the
 * desired contract, not an accident.
 */
public abstract class BaseAvroPayload implements Serializable, KryoSerializable {
  /**
   * Avro data extracted from the source converted to bytes.
   */
  private byte[] recordBytes;

  /**
   * For purposes of preCombining.
   */
  @Getter
  protected Comparable orderingVal;

  protected boolean isDeletedRecord;

  private transient GenericRecord record;

  /**
   * The record's true (writer) schema, captured once when the payload is constructed with an in-memory
   * record and never re-derived from {@code record.getSchema()} afterwards. This is deliberate: {@link
   * #getRecord(Schema)} may overwrite {@link #record} with a schema-PROJECTED decode result (whose schema
   * is whatever the caller requested, not the schema {@link #recordBytes} was actually encoded with) -- if
   * a later call re-derived the writer schema from {@code record.getSchema()} at that point, it would
   * decode the (still original-schema) bytes as if they used the projected schema, silently corrupting the
   * result. Deliberately {@code transient}: not needed (or available) on the Kryo/bytes-only path, see
   * {@link #getRecord(Schema)}.
   */
  private transient Schema writerSchema;

  /**
   * Instantiate {@link BaseAvroPayload}.
   *
   * @param record      Generic record for the payload.
   * @param orderingVal {@link Comparable} to be used in pre combine.
   */
  public BaseAvroPayload(GenericRecord record, Comparable orderingVal) {
    this.record = record;
    this.recordBytes = null; // only initialized when needed
    this.writerSchema = record == null ? null : record.getSchema();
    this.orderingVal = orderingVal;
    this.isDeletedRecord = record == null || isDeleteRecord(record);

    if (orderingVal == null) {
      throw new HoodieException("Ordering value is null for record: " + record);
    }
  }

  /**
   * Defines whether this implementation of {@link HoodieRecordPayload} is deleted.
   * We will not do deserialization in this method.
   */
  public boolean isDeleted(Schema schema, Properties props) {
    return isDeletedRecord;
  }

  /**
   * Defines whether this implementation of {@link HoodieRecordPayload} could produce
   * {@link HoodieRecord#SENTINEL}
   */
  public boolean canProduceSentinel() {
    return false;
  }

  /**
   * @param genericRecord instance of {@link GenericRecord} of interest.
   * @returns {@code true} if record represents a delete record. {@code false} otherwise.
   */
  protected boolean isDeleteRecord(GenericRecord genericRecord) {
    final String isDeleteKey = HoodieRecord.HOODIE_IS_DELETED_FIELD;
    // Modify to be compatible with new version Avro.
    // The new version Avro throws for GenericRecord.get if the field name
    // does not exist in the schema.
    if (genericRecord.getSchema().getField(isDeleteKey) == null) {
      return false;
    }
    Object deleteMarker = genericRecord.get(isDeleteKey);
    return (deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }

  public byte[] getRecordBytes() {
    if (recordBytes == null) {
      if (record == null) {
        recordBytes = new byte[0];
      } else {
        recordBytes = HoodieAvroUtils.avroToBytes(record);
      }
    }
    return recordBytes;
  }

  public Option<IndexedRecord> getIndexedRecord(Schema schema, Properties properties) throws IOException {
    return getRecord(schema);
  }

  protected boolean isEmptyRecord() {
    if (recordBytes == null) {
      return record == null;
    }
    return recordBytes.length == 0;
  }

  protected Option<IndexedRecord> getRecord(Schema schema) throws IOException {
    if (record != null) {
      // NOTE: this is a reference-equality fast path, not a schema-compatibility check; a miss here just
      // means we fall through to one of the decode paths below.
      if (record.getSchema() == schema) {
        return Option.of(record);
      }
      recordBytes = getRecordBytes();
      if (canResolveByName(schema)) {
        // Every requested field exists in the true writer schema by name: the requested schema is a
        // projection/reorder (e.g. meta-fields-only or data-fields-only projection), for which the legacy
        // positional decode below would misalign fields. Decode using `writerSchema` -- NOT
        // `record.getSchema()` -- and let Avro's schema resolution project the decoded fields into `schema`
        // by name. `writerSchema` is used deliberately because `record` gets overwritten with the decode
        // result: on a later call with yet another schema, `record.getSchema()` would then be that
        // PROJECTION's schema, not the schema `recordBytes` was actually encoded with, causing the bytes to
        // be decoded at the wrong byte offsets (silent positional garbage or a hard decoding exception such
        // as AvroRuntimeException "Malformed data. Length is negative"). `writerSchema` stays fixed to the
        // true writer schema across any number of such calls, so this stays correct regardless of call order.
        record = HoodieAvroUtils.bytesToAvro(recordBytes, writerSchema, schema);
        return Option.of(record);
      }
      // The requested schema has fields the writer schema does not (renamed/added columns from schema
      // evolution). Keep the pre-existing positional decode: merge/read paths (e.g. the Hive/Presto MoR
      // realtime readers) rely on it to carry values across renames by position.
      record = SerializableIndexedRecord.fromAvroBytes(schema, recordBytes);
      return Option.of(record);
    }
    if (recordBytes == null || recordBytes.length == 0) {
      return Option.empty();
    }
    // `record` was never materialized in-memory here (e.g. this payload was restored purely from Kryo
    // bytes via #read(Kryo, Input)), so there is no true writer schema available without changing the
    // Kryo wire format to persist one alongside the bytes -- out of scope given the compatibility risk to
    // already-serialized/shuffled payloads. Fall back to the pre-existing behavior (decode with `schema`
    // as both writer and reader); if `schema` genuinely differs from the bytes' true writer schema, Avro
    // reads the length-prefixed fields at the wrong byte offsets, which is just as likely to throw a hard
    // decoding exception (e.g. AvroRuntimeException "Malformed data. Length is negative") as it is to
    // silently return positionally-misaligned garbage -- which one occurs depends on the specific byte
    // layout, not on anything this code controls, so callers must not assume either outcome is safe.
    // Record `schema` as the writer schema for any FUTURE call on this instance too, so a later call (which
    // will now see `record != null`) has a non-null `writerSchema` to decode with instead of NPE-ing --
    // this keeps this fallback's known limitation (misdecoding/exception across genuinely different
    // schemas) but never crashes with a NullPointerException specifically.
    writerSchema = schema;
    record = SerializableIndexedRecord.fromAvroBytes(schema, recordBytes);
    return Option.of(record);
  }

  /**
   * Decides whether the requested schema can be decoded from the true writer schema by Avro's name-based
   * schema resolution, i.e. whether every requested field exists in the writer schema by name (a pure
   * projection/reorder).
   *
   * @return true if name-based resolution applies; false if the requested schema has fields the writer
   * schema does not (schema evolution: renamed/added columns) and the legacy positional decode must be
   * used instead.
   * @throws AvroTypeException if a requested field is missing from the writer schema and has no default
   * value, since it can be resolved neither by name nor positionally.
   */
  private boolean canResolveByName(Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      if (writerSchema.getField(field.name()) == null) {
        if (field.defaultVal() == null) {
          throw new AvroTypeException(String.format(
              "Field '%s' in the requested schema does not exist in the record's writer schema and has no default value; "
                  + "writer schema: %s, requested schema: %s", field.name(), writerSchema, schema));
        }
        return false;
      }
    }
    return true;
  }

  @Override
  public void write(Kryo kryo, Output output) {
    byte[] bytes = getRecordBytes();
    output.writeInt(bytes.length);
    output.writeBytes(bytes);
    kryo.writeClassAndObject(output, orderingVal);
    output.writeBoolean(isDeletedRecord);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int length = input.readInt();
    this.recordBytes = input.readBytes(length);
    this.orderingVal = (Comparable) kryo.readClassAndObject(input);
    this.isDeletedRecord = input.readBoolean();
  }
}
