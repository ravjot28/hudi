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

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecordLocation.isPositionValid;
import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * DataBlock contains a list of records serialized using formats compatible with the base file format.
 * For each base file format there is a corresponding DataBlock format.
 * <p>
 * The Datablock contains:
 *   1. Data Block version
 *   2. Total number of records in the block
 *   3. Actual serialized content of the records
 */
public abstract class HoodieDataBlock extends HoodieLogBlock {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieDataBlock.class);

  // TODO rebase records/content to leverage Either to warrant
  //      that they are mutex (used by read/write flows respectively)
  private final Option<List<HoodieRecord>> records;

  /**
   * Key field's name w/in the record's schema
   */
  private final String keyFieldName;

  private final boolean enablePointLookups;

  protected Schema readerSchema;
  protected final boolean shouldWriteRecordPositions;

  //  Map of string schema to parsed schema.
  private static ConcurrentHashMap<String, Schema> schemaMap = new ConcurrentHashMap<>();

  /**
   * NOTE: This ctor is used on the write-path (ie when records ought to be written into the log)
   */
  public HoodieDataBlock(List<HoodieRecord> records,
                         boolean shouldWriteRecordPositions,
                         Map<HeaderMetadataType, String> header,
                         Map<HeaderMetadataType, String> footer,
                         String keyFieldName) {
    super(header, footer, Option.empty(), Option.empty(), null, false);
    if (shouldWriteRecordPositions) {
      records.sort((o1, o2) -> {
        long v1 = o1.getCurrentPosition();
        long v2 = o2.getCurrentPosition();
        return Long.compare(v1, v2);
      });
      if (isPositionValid(records.get(0).getCurrentPosition())) {
        addRecordPositionsToHeader(
            records.stream().map(HoodieRecord::getCurrentPosition).collect(Collectors.toSet()),
            records.size());
      } else {
        LOG.warn("There are records without valid positions. "
            + "Skip writing record positions to the data block header.");
      }
    }
    this.records = Option.of(records);
    this.keyFieldName = keyFieldName;
    // If no reader-schema has been provided assume writer-schema as one
    this.readerSchema = getWriterSchema(super.getLogBlockHeader());
    this.shouldWriteRecordPositions = shouldWriteRecordPositions;
    this.enablePointLookups = false;
  }

  /**
   * NOTE: This ctor is used on the write-path (ie when records ought to be written into the log)
   */
  protected HoodieDataBlock(Option<byte[]> content,
                            FSDataInputStream inputStream,
                            boolean readBlockLazily,
                            Option<HoodieLogBlockContentLocation> blockContentLocation,
                            Option<Schema> readerSchema,
                            Map<HeaderMetadataType, String> headers,
                            Map<HeaderMetadataType, String> footer,
                            String keyFieldName,
                            boolean enablePointLookups) {
    super(headers, footer, blockContentLocation, content, inputStream, readBlockLazily);
    // Setting `shouldWriteRecordPositions` to false as this constructor is only used by the reader
    this.shouldWriteRecordPositions = false;
    this.records = Option.empty();
    this.keyFieldName = keyFieldName;
    // If no reader-schema has been provided assume writer-schema as one
    this.readerSchema = readerSchema.orElseGet(() -> getWriterSchema(super.getLogBlockHeader()));
    this.enablePointLookups = enablePointLookups;
  }

  @Override
  public byte[] getContentBytes() throws IOException {
    // In case this method is called before realizing records from content
    Option<byte[]> content = getContent();

    checkState(content.isPresent() || records.isPresent(), "Block is in invalid state");

    if (content.isPresent()) {
      return content.get();
    }

    return serializeRecords(records.get());
  }

  public String getKeyFieldName() {
    return keyFieldName;
  }

  protected static Schema getWriterSchema(Map<HeaderMetadataType, String> logBlockHeader) {
    return new Schema.Parser().parse(logBlockHeader.get(HeaderMetadataType.SCHEMA));
  }

  /**
   * Returns all the records iterator contained w/in this block.
   */
  public final <T> ClosableIterator<HoodieRecord<T>> getRecordIterator(HoodieRecordType type) {
    if (records.isPresent()) {
      // TODO need convert record type
      return list2Iterator(unsafeCast(records.get()));
    }
    try {
      // in case records are absent, read content lazily and then convert to IndexedRecords
      return readRecordsFromBlockPayload(type);
    } catch (IOException io) {
      throw new HoodieIOException("Unable to convert content bytes to records", io);
    }
  }

  public Schema getSchema() {
    return readerSchema;
  }

  /**
   * Batch get of keys of interest. Implementation can choose to either do full scan and return matched entries or
   * do a seek based parsing and return matched entries.
   *
   * @param keys keys of interest.
   * @return List of IndexedRecords for the keys of interest.
   * @throws IOException in case of failures encountered when reading/parsing records
   */
  public final <T> ClosableIterator<HoodieRecord<T>> getRecordIterator(List<String> keys, boolean fullKey, HoodieRecordType type) throws IOException {
    boolean fullScan = keys.isEmpty();
    if (enablePointLookups && !fullScan) {
      return lookupRecords(keys, fullKey);
    }

    // Otherwise, we fetch all the records and filter out all the records, but the
    // ones requested
    ClosableIterator<HoodieRecord<T>> allRecords = getRecordIterator(type);
    if (fullScan) {
      return allRecords;
    }

    HashSet<String> keySet = new HashSet<>(keys);
    return FilteringIterator.getInstance(allRecords, keySet, fullKey, this::getRecordKey);
  }

  /**
   * Returns all the records in the type of engine-specific record representation contained
   * within this block in an iterator.
   *
   * @param readerContext {@link HoodieReaderContext} instance with type T.
   * @param <T>           The type of engine-specific record representation to return.
   * @return An iterator containing all records in specified type.
   */
  public final <T> ClosableIterator<T> getEngineRecordIterator(HoodieReaderContext<T> readerContext) {
    if (records.isPresent()) {
      return list2Iterator(unsafeCast(
          records.get().stream().map(hoodieRecord -> (T) hoodieRecord.getData())
              .collect(Collectors.toList())));
    }
    try {
      return readRecordsFromBlockPayload(readerContext);
    } catch (IOException io) {
      throw new HoodieIOException("Unable to convert content bytes to records", io);
    }
  }

  /**
   * Batch get of keys of interest. Implementation can choose to either do full scan and return matched entries or
   * do a seek based parsing and return matched entries.
   *
   * @param readerContext {@link HoodieReaderContext} instance with type T.
   * @param keys          Keys of interest.
   * @param fullKey       Whether the key is full or not.
   * @param <T>           The type of engine-specific record representation to return.
   * @return An iterator containing the records of interest in specified type.
   */
  public final <T> ClosableIterator<T> getEngineRecordIterator(HoodieReaderContext<T> readerContext, List<String> keys, boolean fullKey) {
    boolean fullScan = keys.isEmpty();

    // Otherwise, we fetch all the records and filter out all the records, but the
    // ones requested
    ClosableIterator<T> allRecords = getEngineRecordIterator(readerContext);
    if (fullScan) {
      return allRecords;
    }

    HashSet<String> keySet = new HashSet<>(keys);
    return FilteringEngineRecordIterator.getInstance(allRecords, keySet, fullKey, record ->
        Option.of(readerContext.getValue(record, readerSchema, keyFieldName).toString()));
  }

  protected <T> ClosableIterator<HoodieRecord<T>> readRecordsFromBlockPayload(HoodieRecordType type) throws IOException {
    if (readBlockLazily && !getContent().isPresent()) {
      // read log block contents from disk
      inflate();
    }

    try {
      return deserializeRecords(getContent().get(), type);
    } finally {
      // Free up content to be GC'd by deflating the block
      deflate();
    }
  }

  protected <T> ClosableIterator<T> readRecordsFromBlockPayload(HoodieReaderContext<T> readerContext) throws IOException {
    if (readBlockLazily && !getContent().isPresent()) {
      // read log block contents from disk
      inflate();
    }

    try {
      return deserializeRecords(readerContext, getContent().get());
    } finally {
      // Free up content to be GC'd by deflating the block
      deflate();
    }
  }

  protected <T> ClosableIterator<HoodieRecord<T>> lookupRecords(List<String> keys, boolean fullKey) throws IOException {
    throw new UnsupportedOperationException(
        String.format("Point lookups are not supported by this Data block type (%s)", getBlockType())
    );
  }

  protected abstract byte[] serializeRecords(List<HoodieRecord> records) throws IOException;

  protected abstract <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecordType type) throws IOException;

  /**
   * Deserializes the content bytes of the data block to the records in engine-specific representation.
   *
   * @param readerContext Hudi reader context with engine-specific implementation.
   * @param content       Content in byte array.
   * @param <T>           Record type of engine-specific representation.
   * @return {@link ClosableIterator} of records in engine-specific representation.
   * @throws IOException upon deserialization error.
   */
  protected abstract <T> ClosableIterator<T> deserializeRecords(HoodieReaderContext<T> readerContext, byte[] content) throws IOException;

  public abstract HoodieLogBlockType getBlockType();

  protected Option<Schema.Field> getKeyField(Schema schema) {
    return Option.ofNullable(schema.getField(keyFieldName));
  }

  protected Option<String> getRecordKey(HoodieRecord record) {
    return Option.ofNullable(record.getRecordKey(readerSchema, keyFieldName));
  }

  protected Schema getSchemaFromHeader() {
    String schemaStr = getLogBlockHeader().get(HeaderMetadataType.SCHEMA);
    schemaMap.computeIfAbsent(schemaStr, (schemaString) -> new Schema.Parser().parse(schemaString));
    return schemaMap.get(schemaStr);
  }

  /**
   * Converts the given list to closable iterator.
   */
  static <T> ClosableIterator<T> list2Iterator(List<T> list) {
    Iterator<T> iterator = list.iterator();
    return new ClosableIterator<T>() {
      @Override
      public void close() {
        // ignored
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        return iterator.next();
      }
    };
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * A {@link ClosableIterator} that supports filtering strategy with given keys.
   * User should supply the key extraction function for fetching string format keys.
   */
  private static class FilteringIterator<T> implements ClosableIterator<HoodieRecord<T>> {
    private final ClosableIterator<HoodieRecord<T>> nested; // nested iterator

    private final Set<String> keys; // the filtering keys
    private final boolean fullKey;

    private final Function<HoodieRecord<T>, Option<String>> keyExtract; // function to extract the key

    private HoodieRecord<T> next;

    private FilteringIterator(ClosableIterator<HoodieRecord<T>> nested, Set<String> keys, boolean fullKey, Function<HoodieRecord<T>, Option<String>> keyExtract) {
      this.nested = nested;
      this.keys = keys;
      this.fullKey = fullKey;
      this.keyExtract = keyExtract;
    }

    public static <T> FilteringIterator<T> getInstance(
        ClosableIterator<HoodieRecord<T>> nested,
        Set<String> keys,
        boolean fullKey,
        Function<HoodieRecord<T>, Option<String>> keyExtract) {
      return new FilteringIterator<>(nested, keys, fullKey, keyExtract);
    }

    @Override
    public void close() {
      this.nested.close();
    }

    @Override
    public boolean hasNext() {
      while (this.nested.hasNext()) {
        this.next = this.nested.next();
        String key = keyExtract.apply(this.next)
            .orElseGet(() -> {
              throw new IllegalStateException(String.format("Record without a key (%s)", this.next));
            });

        if (fullKey && keys.contains(key)
            || !fullKey && keys.stream().anyMatch(key::startsWith)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public HoodieRecord<T> next() {
      return this.next;
    }
  }

  /**
   * A {@link ClosableIterator} that supports filtering strategy with given keys on records
   * of engine-specific type.
   * User should supply the key extraction function for fetching string format keys.
   *
   * @param <T> The type of engine-specific record representation.
   */
  private static class FilteringEngineRecordIterator<T> implements ClosableIterator<T> {
    private final ClosableIterator<T> nested; // nested iterator

    private final Set<String> keys; // the filtering keys
    private final boolean fullKey;

    private final Function<T, Option<String>> keyExtract; // function to extract the key

    private T next;

    private FilteringEngineRecordIterator(ClosableIterator<T> nested,
                                          Set<String> keys, boolean fullKey,
                                          Function<T, Option<String>> keyExtract) {
      this.nested = nested;
      this.keys = keys;
      this.fullKey = fullKey;
      this.keyExtract = keyExtract;
    }

    public static <T> FilteringEngineRecordIterator<T> getInstance(
        ClosableIterator<T> nested,
        Set<String> keys,
        boolean fullKey,
        Function<T, Option<String>> keyExtract) {
      return new FilteringEngineRecordIterator<>(nested, keys, fullKey, keyExtract);
    }

    @Override
    public void close() {
      this.nested.close();
    }

    @Override
    public boolean hasNext() {
      while (this.nested.hasNext()) {
        this.next = this.nested.next();
        String key = keyExtract.apply(this.next)
            .orElseGet(() -> {
              throw new IllegalStateException(String.format("Record without a key (%s)", this.next));
            });

        if (fullKey && keys.contains(key)
            || !fullKey && keys.stream().anyMatch(key::startsWith)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public T next() {
      return this.next;
    }
  }
}
