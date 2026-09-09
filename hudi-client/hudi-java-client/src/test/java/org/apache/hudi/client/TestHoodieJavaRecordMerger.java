/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.client.transaction.BucketIndexConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Java ingestion, queries and table services must use the same configured merger on native Avro input. */
public class TestHoodieJavaRecordMerger extends HoodieJavaClientTestHarness {
  private static final HoodieSchema SCHEMA = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"native_record\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"part\",\"type\":\"string\"},"
      + "{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"priority\",\"type\":\"int\"},"
      + "{\"name\":\"value\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

  private String readMergerClass = PriorityMerger.class.getName();

  @Override
  protected void initMetaClient() {
    // Each test creates a table with its own persisted merging contract.
  }

  static Stream<Arguments> mergeModesAndBaseFiles() {
    return Arrays.stream(RecordMergeMode.values()).flatMap(mode -> Stream.of(
        Arguments.of(mode, false), Arguments.of(mode, true)));
  }

  static Stream<Arguments> nativeWriteOperations() {
    return Arrays.stream(RecordMergeMode.values()).flatMap(mode -> Stream.of(
        Arguments.of(mode, WriteOperationType.INSERT), Arguments.of(mode, WriteOperationType.UPSERT),
        Arguments.of(mode, WriteOperationType.BULK_INSERT)));
  }

  @ParameterizedTest
  @MethodSource("mergeModesAndBaseFiles")
  void testOrderingSurvivesLogCompactionAndCompaction(RecordMergeMode mode, boolean compactFirst) throws Exception {
    HoodieWriteConfig config = config(mode);
    initialize(config, mode, Option.empty());
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      write(client, WriteOperationType.INSERT, record("a", 50, 50, "event-winner"));
      if (compactFirst) {
        compact(client);
      }
      write(client, WriteOperationType.UPSERT, record("a", 10, 90, "priority-winner"));
      write(client, WriteOperationType.UPSERT, record("a", 20, 20, "commit-winner"));
      String expected = mode == RecordMergeMode.EVENT_TIME_ORDERING ? "event-winner"
          : mode == RecordMergeMode.CUSTOM ? "priority-winner" : "commit-winner";
      PriorityMerger.CALLS.set(0);
      assertEquals(Collections.singletonMap("a", expected), snapshot());
      if (mode == RecordMergeMode.CUSTOM) {
        assertTrue(PriorityMerger.CALLS.get() > 0, "snapshot must execute the configured merger");
      }
      Option<String> logCompaction = client.scheduleLogCompaction(Option.empty());
      assertTrue(logCompaction.isPresent(), "fixture must create eligible log blocks");
      HoodieWriteMetadata<List<WriteStatus>> metadata = client.logCompact(logCompaction.get(), true);
      assertStatuses(metadata.getWriteStatuses());
      assertEquals(Collections.singletonMap("a", expected), snapshot());
      // Leave an additional log to exercise the base/log merger even after log compaction.
      write(client, WriteOperationType.UPSERT, record("a", 20, 20, "commit-winner"));
      PriorityMerger.CALLS.set(0);
      compact(client);
      if (mode == RecordMergeMode.CUSTOM) {
        assertTrue(PriorityMerger.CALLS.get() > 0, "compaction must execute the configured merger");
      }
      assertEquals(Collections.singletonMap("a", expected), optimized());
      assertEquals(optimized(), snapshot());
    }
  }

  @ParameterizedTest
  @MethodSource("nativeWriteOperations")
  void testNativeWritesDeleteAndRollback(RecordMergeMode mode, WriteOperationType operation) throws Exception {
    HoodieWriteConfig config = config(mode);
    initialize(config, mode, Option.empty());
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      write(client, operation, record("a", 1, 1, "first"), record("b", 1, 1, "untouched"));
      assertEquals(2, snapshot().size());
      String update = write(client, WriteOperationType.UPSERT, record("a", 2, 2, "updated"));
      assertEquals("updated", snapshot().get("a"));
      assertTrue(client.rollback(update));
      assertEquals("first", snapshot().get("a"));
      String deletion = client.startCommit();
      List<WriteStatus> deleted = client.delete(Collections.singletonList(new HoodieKey("a", "p")), deletion);
      assertStatuses(deleted);
      assertTrue(client.commit(deletion, deleted));
      assertEquals(Collections.singletonMap("b", "untouched"), snapshot());
      write(client, WriteOperationType.UPSERT, record("a", 3, 3, "resurrected"));
      compact(client);
      assertEquals("resurrected", optimized().get("a"));
      assertEquals("untouched", optimized().get("b"));
      assertEquals(optimized(), snapshot());
    }
  }

  @ParameterizedTest
  @CsvSource({"EVENT_TIME_ORDERING,false", "EVENT_TIME_ORDERING,true", "CUSTOM,false", "CUSTOM,true"})
  void testDuplicateReductionUsesConfiguredSemantics(RecordMergeMode mode, boolean reverse) throws Exception {
    HoodieWriteConfig config = config(mode);
    initialize(config, mode, Option.empty());
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      List<HoodieRecord<IndexedRecord>> records = new ArrayList<>(Arrays.asList(
          record("a", 90, 10, "event"), record("a", 10, 90, "priority"), record("a", 1, 1, "stale")));
      if (reverse) {
        Collections.reverse(records);
      }
      PriorityMerger.CALLS.set(0);
      write(client, WriteOperationType.UPSERT, records);
      if (mode == RecordMergeMode.CUSTOM) {
        assertTrue(PriorityMerger.CALLS.get() > 0, "input reduction must execute the custom merger");
      }
      Map<String, String> expected = Collections.singletonMap("a", mode == RecordMergeMode.CUSTOM ? "priority" : "event");
      assertEquals(expected, snapshot());
      compact(client);
      assertEquals(expected, optimized());
    }
  }

  @ParameterizedTest
  @MethodSource("mergeModesAndBaseFiles")
  void testOrderingTiesDeletesAndResurrection(RecordMergeMode mode, boolean compactFirst) throws Exception {
    HoodieWriteConfig config = config(mode);
    initialize(config, mode, Option.empty());
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      write(client, WriteOperationType.INSERT, record("a", 50, 50, "first"));
      if (compactFirst) {
        compact(client);
      }
      write(client, WriteOperationType.UPSERT, record("a", 50, 50, "tie"));
      assertEquals(Collections.singletonMap("a", "tie"), snapshot());
      HoodieRecord<IndexedRecord> staleDelete = tombstone(10);
      write(client, WriteOperationType.UPSERT, staleDelete);
      if (mode == RecordMergeMode.COMMIT_TIME_ORDERING) {
        assertTrue(snapshot().isEmpty());
      } else {
        assertEquals(Collections.singletonMap("a", "tie"), snapshot());
      }
      write(client, WriteOperationType.UPSERT, tombstone(100));
      assertTrue(snapshot().isEmpty());
      compact(client);
      assertTrue(optimized().isEmpty());
      write(client, WriteOperationType.UPSERT, record("a", 200, 200, "back"));
      assertEquals(Collections.singletonMap("a", "back"), snapshot());
      compact(client);
      assertEquals(snapshot(), optimized());
    }
  }

  @ParameterizedTest
  @CsvSource({"EVENT_TIME_ORDERING,false", "EVENT_TIME_ORDERING,true", "COMMIT_TIME_ORDERING,false", "COMMIT_TIME_ORDERING,true"})
  void testNativePartialUpdates(RecordMergeMode mode, boolean compactFirst) throws Exception {
    HoodieWriteConfig config = config(mode);
    initialize(config, mode, Option.of(PartialUpdateMode.IGNORE_DEFAULTS));
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      write(client, WriteOperationType.INSERT, record("a", 50, 50, "preserve"));
      if (compactFirst) {
        compact(client);
      }
      write(client, WriteOperationType.UPSERT, record("a", 60, 60, null));
      assertEquals(Collections.singletonMap("a", "preserve"), snapshot());
      assertField("a", "ts", "60");
      assertField("a", "priority", "60");
      compact(client);
      assertEquals(Collections.singletonMap("a", "preserve"), optimized());
      assertEquals(optimized(), snapshot());
    }
  }

  @ParameterizedTest
  @CsvSource({"BITCASK,false", "BITCASK,true", "ROCKS_DB,false", "ROCKS_DB,true"})
  void testCustomMergerSpillAndEarlyClose(String diskMapType, boolean closeEarly) throws Exception {
    HoodieWriteConfig config = config(RecordMergeMode.CUSTOM);
    initialize(config, RecordMergeMode.CUSTOM, Option.empty());
    Map<String, String> expected = new HashMap<>();
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      List<HoodieRecord<IndexedRecord>> first = new ArrayList<>();
      List<HoodieRecord<IndexedRecord>> updates = new ArrayList<>();
      for (int i = 0; i < 128; i++) {
        first.add(record("key" + i, 90, 1, "old"));
        updates.add(record("key" + i, 1, 90, "selected"));
        expected.put("key" + i, "selected");
      }
      write(client, WriteOperationType.INSERT, first);
      write(client, WriteOperationType.UPSERT, updates);
      Path spill = tempDir.resolve("native-spill");
      Files.createDirectories(spill);
      TypedProperties props = readProperties();
      props.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), "1");
      props.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), spill.toString());
      props.setProperty(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), diskMapType);
      try (HoodieJavaReadClient reader = new HoodieJavaReadClient(context, metaClient, props);
           ClosableIterator<HoodieRecord<IndexedRecord>> rows = reader.readSnapshot()) {
        assertTrue(rows.hasNext());
        try (Stream<Path> files = Files.list(spill)) {
          assertTrue(files.findAny().isPresent(), "the test must actually spill");
        }
        if (closeEarly) {
          assertNotNull(rows.next());
        } else {
          assertEquals(expected, values(rows));
        }
      }
      assertSpillEmpty(spill);
      assertEquals(expected, snapshot());
      props.setProperty(PriorityMerger.FAIL, "true");
      PriorityMerger.CALLS.set(0);
      try (HoodieJavaReadClient reader = new HoodieJavaReadClient(context, metaClient, props)) {
        assertThrows(RuntimeException.class, () -> values(reader.readSnapshot()), "merger exceptions must reach the caller");
      }
      assertTrue(PriorityMerger.CALLS.get() > 0, "failure must originate in the custom merger");
      assertSpillEmpty(spill);
      assertEquals(expected, snapshot(), "a failed merger must not poison a subsequent reader");
    }
  }

  @Test
  void testMissingAndMismatchedCustomMergerFailInsteadOfFallingBack() throws Exception {
    HoodieWriteConfig config = config(RecordMergeMode.CUSTOM);
    initialize(config, RecordMergeMode.CUSTOM, Option.empty());
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      write(client, WriteOperationType.INSERT, record("a", 90, 1, "event"));
      write(client, WriteOperationType.UPSERT, record("a", 1, 90, "priority"));
      for (String implementation : Arrays.asList("", "no.such.Merger", WrongStrategyMerger.class.getName())) {
        TypedProperties props = new TypedProperties();
        props.setProperty(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), implementation);
        try (HoodieJavaReadClient reader = new HoodieJavaReadClient(context, metaClient, props)) {
          assertThrows(RuntimeException.class, () -> values(reader.readSnapshot()));
        }
      }
      assertEquals(Collections.singletonMap("a", "priority"), snapshot());
    }
  }

  @ParameterizedTest
  @EnumSource(RecordMergeMode.class)
  void testTimeTravelAndIncrementalNativeReads(RecordMergeMode mode) throws Exception {
    HoodieWriteConfig config = config(mode);
    initialize(config, mode, Option.empty());
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      String first = write(client, WriteOperationType.INSERT, record("a", 90, 10, "event"));
      String second = write(client, WriteOperationType.UPSERT, record("a", 10, 90, "priority"));
      String completion = metaClient.getActiveTimeline().filterCompletedInstants().getInstantsAsStream()
          .filter(instant -> instant.requestedTime().equals(second)).findFirst().get().getCompletionTime();
      Map<String, String> expected = Collections.singletonMap("a", mode == RecordMergeMode.EVENT_TIME_ORDERING ? "event" : "priority");
      try (HoodieJavaReadClient reader = new HoodieJavaReadClient(context, metaClient, readProperties())) {
        assertEquals(Collections.singletonMap("a", "event"), values(reader.readSnapshot(first)));
        assertEquals(expected, values(reader.readSnapshot(second)));
        assertEquals(expected, values(reader.readIncremental("earliest", completion)));
        assertEquals(expected, values(reader.readIncremental("earliest", null)));
      }
      compact(client);
      assertEquals(expected, snapshot());
      assertEquals(expected, optimized());
    }
  }

  @ParameterizedTest
  @EnumSource(RecordMergeMode.class)
  void testSchemaEvolutionOnNativeRecords(RecordMergeMode mode) throws Exception {
    HoodieWriteConfig config = config(mode);
    initialize(config, mode, Option.empty());
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      write(client, WriteOperationType.INSERT, record("a", 1, 1, "old"), record("b", 1, 1, "untouched"));
      compact(client);
    }
    HoodieSchema evolved = HoodieSchema.parse(SCHEMA.toString().replace(
        "\"fields\":[", "\"fields\":[{\"name\":\"added\",\"type\":[\"null\",\"string\"],\"default\":null},"));
    assertTrue(evolved.getField("added").isPresent());
    HoodieWriteConfig evolvedConfig = HoodieWriteConfig.newBuilder().withEngineType(EngineType.JAVA)
        .withProps(config.getProps()).withSchema(evolved.toString()).build();
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, evolvedConfig)) {
      HoodieRecord<IndexedRecord> update = record(evolved, "a", 2, 2, "new");
      update.getData().put(evolved.toAvroSchema().getField("added").pos(), "added-value");
      write(client, WriteOperationType.UPSERT, update);
      assertEquals("new", snapshot().get("a"));
      assertField("a", "added", "added-value");
      assertField("b", "added", null);
      compact(client);
      assertEquals(optimized(), snapshot());
      assertField("a", "added", "added-value");
      assertField("b", "added", null);
    }
  }

  @ParameterizedTest
  @EnumSource(RecordMergeMode.class)
  void testMetadataDeclaringNativeSchema(RecordMergeMode mode) throws Exception {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(SCHEMA);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withEngineType(EngineType.JAVA)
        .withProps(config(mode).getProps()).withSchema(schema.toString()).build();
    initialize(config, mode, Option.empty());
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      write(client, WriteOperationType.INSERT, record(schema, "a", 1, 1, "first"));
      write(client, WriteOperationType.UPSERT, record(schema, "a", 2, 2, "updated"));
      assertEquals(Collections.singletonMap("a", "updated"), snapshot());
      String configuredSchema = client.getConfig().getSchema();
      compact(client);
      assertEquals(configuredSchema, client.getConfig().getSchema());
      write(client, WriteOperationType.UPSERT, record(schema, "a", 3, 3, "after-compaction"));
      assertEquals(Collections.singletonMap("a", "after-compaction"), snapshot());
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void testPartialSchemaRequiresAndInvokesCustomCallback(boolean compactFirst, boolean implementsPartialMerge) throws Exception {
    readMergerClass = implementsPartialMerge ? FieldPatchMerger.class.getName() : PriorityMerger.class.getName();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withEngineType(EngineType.JAVA).withProps(config(RecordMergeMode.CUSTOM).getProps())
        .withRecordMergeImplClasses(readMergerClass).build();
    initialize(config, RecordMergeMode.CUSTOM, Option.empty());
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      write(client, WriteOperationType.INSERT, record("a", 1, 1, "preserved"));
      if (compactFirst) {
        compact(client);
      }
      HoodieSchema partialSchema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"native_record\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"part\",\"type\":\"string\"},"
          + "{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"priority\",\"type\":\"int\"}]}");
      // Simulate the partial-schema configuration produced by ingestion schema reconciliation.
      client.getConfig().getProps().setProperty(HoodieWriteConfig.WRITE_PARTIAL_UPDATE_SCHEMA.key(), partialSchema.toString());
      write(client, WriteOperationType.UPSERT, record(partialSchema, "a", 2, 2, null));
      client.getConfig().getProps().remove(HoodieWriteConfig.WRITE_PARTIAL_UPDATE_SCHEMA.key());
      if (implementsPartialMerge) {
        FieldPatchMerger.PARTIAL_CALLS.set(0);
        assertEquals(Collections.singletonMap("a", "preserved"), snapshot());
        assertTrue(FieldPatchMerger.PARTIAL_CALLS.get() > 0, "partial log blocks must invoke partialMerge");
        FieldPatchMerger.PARTIAL_CALLS.set(0);
        compact(client);
        assertTrue(FieldPatchMerger.PARTIAL_CALLS.get() > 0, "compaction must also invoke partialMerge");
        assertEquals(Collections.singletonMap("a", "preserved"), optimized());
      } else {
        RuntimeException failure = assertThrows(RuntimeException.class, this::snapshot);
        Throwable cause = failure;
        while (cause.getCause() != null) {
          cause = cause.getCause();
        }
        assertTrue(cause instanceof UnsupportedOperationException);
        assertTrue(cause.getMessage().contains("Partial merging logic is not implemented"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"EVENT_TIME_ORDERING,false", "EVENT_TIME_ORDERING,true", "COMMIT_TIME_ORDERING,false", "COMMIT_TIME_ORDERING,true"})
  void testOutOfOrderPartialUpdatesAreIndependentOfCompaction(RecordMergeMode mode, boolean compactFirst) throws Exception {
    HoodieWriteConfig config = config(mode);
    initialize(config, mode, Option.of(PartialUpdateMode.IGNORE_DEFAULTS));
    try (HoodieJavaWriteClient<IndexedRecord> client = new HoodieJavaWriteClient<>(context, config)) {
      write(client, WriteOperationType.INSERT, record("a", 50, 50, "preserve"));
      if (compactFirst) {
        compact(client);
      }
      write(client, WriteOperationType.UPSERT, record("a", 60, 60, null));
      assertEquals(Collections.singletonMap("a", "preserve"), snapshot());
      write(client, WriteOperationType.UPSERT, record("a", 10, 10, "late"));
      String expected = mode == RecordMergeMode.EVENT_TIME_ORDERING ? "preserve" : "late";
      assertEquals(Collections.singletonMap("a", expected), snapshot());
      compact(client);
      assertEquals(Collections.singletonMap("a", expected), optimized());
      assertEquals(optimized(), snapshot());
    }
  }

  private HoodieWriteConfig config(RecordMergeMode mode) {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "part");
    props.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    return HoodieWriteConfig.newBuilder().withEngineType(EngineType.JAVA).withPath(basePath).forTable("native_merger")
        .withSchema(SCHEMA.toString()).withProps(props).withRecordMergeMode(mode)
        .withRecordMergeImplClasses(PriorityMerger.class.getName())
        .withRecordMergeStrategyId(HoodieRecordMerger.getRecordMergeStrategyId(mode, null, PriorityMerger.ID,
            HoodieTableVersion.NINE))
        .combineInput(true, true)
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(HoodieIndex.IndexType.BUCKET).withBucketNum("1").build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1)
            .withLogCompactionEnabled(true).withLogCompactionBlocksThreshold(1).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL)
        .withMarkersType(MarkerType.DIRECT.name()).withEmbeddedTimelineServerEnabled(false)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new BucketIndexConcurrentFileWritesConflictResolutionStrategy()).build())
        .build();
  }

  private void initialize(HoodieWriteConfig config, RecordMergeMode mode, Option<PartialUpdateMode> partialMode) throws IOException {
    metaClient = HoodieTableMetaClient.newTableBuilder().fromProperties(config.getProps())
        .setTableName("native_merger").setTableType(HoodieTableType.MERGE_ON_READ)
        .setPayloadClassName(null).setRecordMergeMode(mode)
        .setRecordMergeStrategyId(mode == RecordMergeMode.CUSTOM ? PriorityMerger.ID : null)
        .initTable(storageConf.newInstance(), basePath);
    if (partialMode.isPresent()) {
      Properties props = new Properties();
      props.setProperty(HoodieTableConfig.PARTIAL_UPDATE_MODE.key(), partialMode.get().name());
      HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), props);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertEquals(partialMode, metaClient.getTableConfig().getPartialUpdateMode());
    }
    assertEquals(mode, metaClient.getTableConfig().getRecordMergeMode());
    if (mode == RecordMergeMode.CUSTOM) {
      assertEquals(PriorityMerger.ID, metaClient.getTableConfig().getRecordMergeStrategyId());
      assertTrue(config.getRecordMerger() instanceof PriorityMerger);
    }
  }

  private HoodieRecord<IndexedRecord> record(String key, long timestamp, int priority, String value) {
    return record(SCHEMA, key, timestamp, priority, value);
  }

  private HoodieRecord<IndexedRecord> record(HoodieSchema schema, String key, long timestamp, int priority, String value) {
    GenericData.Record data = new GenericData.Record(schema.toAvroSchema());
    data.put("id", key);
    data.put("part", "p");
    data.put("ts", timestamp);
    data.put("priority", priority);
    if (schema.getField("value").isPresent()) {
      data.put("value", value);
    }
    return new HoodieAvroIndexedRecord(new HoodieKey(key, "p"), data, timestamp);
  }

  private HoodieRecord<IndexedRecord> tombstone(long timestamp) {
    return new HoodieAvroIndexedRecord(new HoodieKey("a", "p"), record("a", timestamp, 0, null).getData(),
        timestamp, HoodieOperation.DELETE, true);
  }

  @SafeVarargs
  private final String write(HoodieJavaWriteClient<IndexedRecord> client, WriteOperationType operation, HoodieRecord<IndexedRecord>... records) {
    return write(client, operation, Arrays.asList(records));
  }

  private String write(HoodieJavaWriteClient<IndexedRecord> client, WriteOperationType operation, List<HoodieRecord<IndexedRecord>> records) {
    String instant = client.startCommit();
    List<WriteStatus> statuses;
    switch (operation) {
      case INSERT:
        statuses = client.insert(records, instant);
        break;
      case BULK_INSERT:
        statuses = client.bulkInsert(records, instant);
        break;
      default:
        statuses = client.upsert(records, instant);
    }
    assertStatuses(statuses);
    assertTrue(client.commit(instant, statuses));
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getActiveTimeline().filterCompletedInstants().containsInstant(instant));
    return instant;
  }

  private void compact(HoodieJavaWriteClient<IndexedRecord> client) {
    Option<String> instant = client.scheduleCompaction(Option.empty());
    assertTrue(instant.isPresent());
    HoodieWriteMetadata<List<WriteStatus>> metadata = client.compact(instant.get());
    assertStatuses(metadata.getWriteStatuses());
    client.commitCompaction(instant.get(), metadata, Option.empty());
    metaClient = HoodieTableMetaClient.reload(metaClient);
  }

  private static void assertStatuses(List<WriteStatus> statuses) {
    for (WriteStatus status : statuses) {
      assertFalse(status.hasErrors(), () -> "Write failed: " + status.getGlobalError() + " " + status.getErrors());
    }
  }

  private TypedProperties readProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), readMergerClass);
    return props;
  }

  private Map<String, String> snapshot() {
    try (HoodieJavaReadClient reader = new HoodieJavaReadClient(context, metaClient, readProperties())) {
      return values(reader.readSnapshot());
    }
  }

  private Map<String, String> optimized() {
    try (HoodieJavaReadClient reader = new HoodieJavaReadClient(context, metaClient, readProperties())) {
      return values(reader.readOptimized());
    }
  }

  private void assertField(String key, String field, String expected) {
    boolean found = false;
    try (HoodieJavaReadClient reader = new HoodieJavaReadClient(context, metaClient, readProperties());
         ClosableIterator<HoodieRecord<IndexedRecord>> rows = reader.readSnapshot()) {
      while (rows.hasNext()) {
        HoodieRecord<IndexedRecord> row = rows.next();
        if (key.equals(row.getRecordKey())) {
          found = true;
          IndexedRecord data = row.getData();
          assertNotNull(data.getSchema().getField(field));
          Object value = data.get(data.getSchema().getField(field).pos());
          assertEquals(expected, value == null ? null : value.toString());
        }
      }
    }
    assertTrue(found, "expected key must be present");
  }

  private Map<String, String> values(ClosableIterator<HoodieRecord<IndexedRecord>> rows) {
    Map<String, String> result = new HashMap<>();
    try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator = rows) {
      while (iterator.hasNext()) {
        HoodieRecord<IndexedRecord> row = iterator.next();
        assertFalse(result.containsKey(row.getRecordKey()), "merged snapshot must have unique keys");
        IndexedRecord data = row.getData();
        Object value = data.get(data.getSchema().getField("value").pos());
        result.put(row.getRecordKey(), value == null ? null : value.toString());
      }
    }
    return result;
  }

  private static void assertSpillEmpty(Path spill) throws IOException {
    try (Stream<Path> files = Files.list(spill)) {
      assertFalse(files.findAny().isPresent(), "all spill resources must be released");
    }
  }

  /** Deliberately differs from both event-time and commit-time ordering: highest priority wins. */
  public static class PriorityMerger implements HoodieRecordMerger {
    static final String ID = "81d0c520-f661-4b57-ae0c-c94e1348143b";
    static final String FAIL = "test.java.record.merger.fail";
    static final AtomicInteger CALLS = new AtomicInteger();

    @Override
    public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, RecordContext<T> context, TypedProperties props) throws IOException {
      CALLS.incrementAndGet();
      if (props.getBoolean(FAIL, false)) {
        throw new IOException("Injected custom merger failure");
      }
      if (newer.isCommitTimeOrderingDelete() || older.isCommitTimeOrderingDelete()) {
        return newer;
      }
      if (older.isDelete() || newer.isDelete()) {
        return older.getOrderingValue().compareTo(newer.getOrderingValue()) > 0 ? older : newer;
      }
      int oldPriority = ((Number) context.getValue(older.getRecord(), context.getSchemaFromBufferRecord(older), "priority")).intValue();
      int newPriority = ((Number) context.getValue(newer.getRecord(), context.getSchemaFromBufferRecord(newer), "priority")).intValue();
      return oldPriority > newPriority ? older : newer;
    }

    @Override
    public HoodieRecord.HoodieRecordType getRecordType() {
      return HoodieRecord.HoodieRecordType.AVRO;
    }

    @Override
    public String getMergingStrategy() {
      return ID;
    }
  }

  public static class WrongStrategyMerger extends PriorityMerger {
    @Override
    public String getMergingStrategy() {
      return "63d9c35e-b7aa-49f1-b7e7-d45b129f36dd";
    }
  }

  /** Tests the partialMerge extension point with a full record and a patch missing the value field. */
  public static class FieldPatchMerger extends PriorityMerger {
    static final AtomicInteger PARTIAL_CALLS = new AtomicInteger();

    @Override
    public <T> BufferedRecord<T> partialMerge(BufferedRecord<T> older, BufferedRecord<T> newer, HoodieSchema readerSchema,
                                             RecordContext<T> context, TypedProperties props) throws IOException {
      PARTIAL_CALLS.incrementAndGet();
      if (older.isDelete() || newer.isDelete()) {
        return merge(older, newer, context, props);
      }
      HoodieSchema oldSchema = context.getSchemaFromBufferRecord(older);
      HoodieSchema newSchema = context.getSchemaFromBufferRecord(newer);
      Object[] fields = new Object[readerSchema.getFields().size()];
      for (HoodieSchemaField field : readerSchema.getFields()) {
        fields[field.pos()] = newSchema.getField(field.name()).isPresent()
            ? context.getValue(newer.getRecord(), newSchema, field.name())
            : oldSchema.getField(field.name()).isPresent() ? context.getValue(older.getRecord(), oldSchema, field.name()) : null;
      }
      return new BufferedRecord<>(newer.getRecordKey(), newer.getOrderingValue(), context.constructEngineRecord(readerSchema, fields),
          context.encodeSchema(readerSchema), newer.getHoodieOperation());
    }
  }
}
