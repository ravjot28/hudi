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

package org.apache.hudi.client;

import org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector;
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.client.transaction.BucketIndexConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.cdc.HoodieCDCExtractor;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieTTLConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.JavaHoodieIndexFactory;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.action.ttl.strategy.PartitionTTLStrategyType;
import org.apache.hudi.table.upgrade.JavaUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableConfig.TYPE;
import static org.apache.hudi.config.HoodieWriteConfig.ENABLE_SCHEMA_CONFLICT_RESOLUTION;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieJavaReadClient}: snapshot, read-optimized, time-travel, and completion-time-based
 * incremental reads on the Java engine, including NBCC (Non-Blocking Concurrency Control) read-side
 * correctness -- this is the read-side validation for the NBCC write support in
 * {@code TestJavaNonBlockingConcurrencyControl}.
 *
 * <p>CDC (change-data-capture) output is out of scope for {@code HoodieJavaReadClient}. CDC-enabled Java
 * MOR writes are nevertheless covered below because Hudi's common CDC extractor can infer their changes
 * from normal MOR log files; what is absent is a standalone Java read API that emits CDC rows.
 */
@Tag("functional")
public class TestHoodieJavaReadClient extends HoodieJavaClientTestHarness {

  // Deliberately does NOT declare the five _hoodie_ meta fields: they are auto-added by the write path via
  // withPopulateMetaFields(true) below. Declaring them explicitly here used to trigger a pre-existing,
  // upstream data-corruption bug on any write that merges against already-persisted data (CoW small-file
  // rewrites, MoR log-over-base-file writes) -- see the hudi-avro-merge-corruption-bug investigation.
  private final String jsonSchema = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"testRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"age\", \"type\": [\"null\", \"int\"]},\n"
      + "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"part\", \"type\": [\"null\", \"string\"]}\n"
      + "  ]\n"
      + "}";

  private HoodieSchema schema;

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @Override
  protected void initMetaClient() {
    // metaClient is (re)initialized per test, once the test-specific HoodieWriteConfig is built.
  }

  @BeforeEach
  public void setUp() {
    schema = HoodieSchema.parse(jsonSchema);
  }

  // -------------------------------------------------------------------------
  //  1. Snapshot merges log files without compaction
  // -------------------------------------------------------------------------

  @Test
  public void testSnapshotMergesLogFilesWithoutCompaction() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1", false);
    writeAndCommit(client, "id1,Danny,23,2,par1", true, WriteOperationType.UPSERT);

    // no compaction yet: both writes are log-only, so a base-file-only read-optimized query must see
    // nothing (asserted directly here, not just implied), but the snapshot must reflect the merged
    // (latest) value.
    try (HoodieJavaReadClient readClient = newReadClient()) {
      assertTrue(readAll(readClient.readOptimized()).isEmpty(),
          "read-optimized must be empty before any compaction has ever run");
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      assertEquals(Collections.singletonMap("id1", "id1,Danny,23,2,par1"), snapshot,
          "snapshot must merge log files without compaction");
    }

    compact(client);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      assertEquals(Collections.singletonMap("id1", "id1,Danny,23,2,par1"), snapshot,
          "snapshot after compaction must still reflect the merged value");
    }
    client.close();
  }

  /** Spark's lexicographic ordering contract must survive Java log merges and base-file rewrites. */
  @ParameterizedTest
  @CsvSource({"8,false", "8,true", "9,false", "9,true", "10,false", "10,true"})
  public void testMultipleOrderingFieldsAcrossCompaction(int version, boolean compactFirst) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withEngineType(EngineType.JAVA)
        .withProps(nbccBucketConfig(RecordMergeMode.EVENT_TIME_ORDERING, EventTimeAvroPayload.class.getName()).getProps())
        .withWriteTableVersion(version)
        .withProps(Collections.singletonMap(HoodieTableConfig.ORDERING_FIELDS.key(), "ts,name")).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      String[] inputs = {"id1,middle,1,10,par1", "id1,aaa,2,10,par1", "id1,zzz,3,10,par1",
          "id1,aaa,4,20,par1", "id1,zzz,5,19,par1"};
      String[] winners = {inputs[0], inputs[0], inputs[2], inputs[3], inputs[3]};
      for (int i = 0; i < inputs.length; i++) {
        GenericRecord record = str2GenericRecord(inputs[i]);
        Comparable ordering = OrderingValues.create(new Comparable[] {(Long) record.get("ts"), (String) record.get("name")});
        String instant = WriteClientTestUtils.createNewInstantTime();
        WriteClientTestUtils.startCommitWithTime(client, instant);
        List<WriteStatus> statuses = client.upsert(Collections.singletonList(new HoodieAvroRecord<>(
            new HoodieKey("id1", "par1"), new EventTimeAvroPayload(record, ordering))), instant);
        assertNoErrors(statuses);
        commit(client, instant, statuses);
        assertEquals(Collections.singletonMap("id1", winners[i]), readAll(reader.readSnapshot()), "ordering after input " + i);
        assertEquals(Collections.singletonList("id1"), readAllRecordKeys(reader.readSnapshot()));
        if (compactFirst) {
          compact(client);
          assertEquals(Collections.singletonMap("id1", winners[i]), readAll(reader.readOptimized()));
        }
      }
      if (!compactFirst) {
        compact(client);
      }
      assertEquals(Collections.singletonMap("id1", inputs[3]), readAll(reader.readOptimized()));
    }
  }

  /**
   * Ports Flink's event-time MOR merge invariant to the standalone reader. A later commit carrying
   * an older ordering value must not replace the newer logical record, either while merging log
   * blocks for a snapshot or while materializing those blocks into a base file during compaction.
   */
  @Test
  public void testSnapshotAndCompactionRespectEventTimeOrdering() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig(
        RecordMergeMode.EVENT_TIME_ORDERING, EventTimeAvroPayload.class.getName());
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeEventTimeAndCommit(client, "id1,newer,40,4,par1", WriteOperationType.INSERT);
    writeEventTimeAndCommit(client, "id1,stale,10,1,par1", WriteOperationType.UPSERT);

    Map<String, String> expected = Collections.singletonMap("id1", "id1,newer,40,4,par1");
    try (HoodieJavaReadClient readClient = newReadClient()) {
      assertEquals(expected, readAll(readClient.readSnapshot()),
          "snapshot merge must keep the record with the greatest event-time ordering value");
    }

    compact(client);
    try (HoodieJavaReadClient readClient = newReadClient()) {
      assertEquals(expected, readAll(readClient.readOptimized()),
          "compaction must materialize the same event-time winner into the base file");
      assertEquals(expected, readAll(readClient.readSnapshot()),
          "snapshot semantics must remain unchanged after compaction");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  2. NBCC two-writer snapshot isolation
  // -------------------------------------------------------------------------

  @Test
  public void testSnapshotIsolationUnderNBCCInflightWriter() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());

    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);
    String instant1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> statuses1 = writeData(client1, instant1, "id1,Danny,1,1,par1", false);

    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    String instant2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> statuses2 = writeData(client2, instant2, "id2,Betty,2,2,par1", false);

    // writer1 commits; writer2 stays inflight (never commits).
    commit(client1, instant1, statuses1);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      assertEquals(Collections.singletonMap("id1", "id1,Danny,1,1,par1"), snapshot,
          "the still-inflight writer2's data must not be visible to a snapshot read");
    }

    // now complete writer2 and re-read.
    commit(client2, instant2, statuses2);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,Danny,1,1,par1");
      expected.put("id2", "id2,Betty,2,2,par1");
      assertEquals(expected, snapshot, "once writer2 completes, its data must become visible");
    }
    client1.close();
    client2.close();
  }

  // -------------------------------------------------------------------------
  //  3. Read-optimized invisible until compaction
  // -------------------------------------------------------------------------

  @Test
  public void testReadOptimizedInvisibleUntilCompaction() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    // bulk_insert id0 first, so bucket 0 has a base file.
    String bulkInsertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> bulkInsertRecords = Collections.singletonList(str2HoodieRecord("id0,Al,0,0,par1"));
    WriteClientTestUtils.startCommitWithTime(client, bulkInsertTime);
    List<WriteStatus> bulkInsertStatuses = client.bulkInsert(bulkInsertRecords, bulkInsertTime);
    assertNoErrors(bulkInsertStatuses);
    commit(client, bulkInsertTime, bulkInsertStatuses);

    // then insert id1: same bucket (numBuckets=1), so this is routed as a log append to the existing
    // bucket 0 file group instead of a new base file.
    writeAndCommit(client, "id1,Danny,1,1,par1", true);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> readOptimized = readAll(readClient.readOptimized());
      assertEquals(Collections.singletonMap("id0", "id0,Al,0,0,par1"), readOptimized,
          "read-optimized must only see the bulk_insert base file, not the log-only id1 update");
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      Map<String, String> expectedSnapshot = new HashMap<>();
      expectedSnapshot.put("id0", "id0,Al,0,0,par1");
      expectedSnapshot.put("id1", "id1,Danny,1,1,par1");
      assertEquals(expectedSnapshot, snapshot, "snapshot must see both the base file and the log update");
    }

    compact(client);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> readOptimized = readAll(readClient.readOptimized());
      Map<String, String> expected = new HashMap<>();
      expected.put("id0", "id0,Al,0,0,par1");
      expected.put("id1", "id1,Danny,1,1,par1");
      assertEquals(expected, readOptimized, "after compaction, read-optimized must see the merged data");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  4. Incremental basic ranges
  // -------------------------------------------------------------------------

  @Test
  public void testIncrementalBasicRanges() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    String c1 = writeAndCommit(client, "id1,Danny,1,1,par1", true);
    String c2 = writeAndCommit(client, "id2,Betty,2,2,par1", true);
    String c3 = writeAndCommit(client, "id3,Chris,3,3,par1", true);

    String completion1 = completionTimeOf(c1);
    String completion2 = completionTimeOf(c2);

    Map<String, String> record1 = Collections.singletonMap("id1", "id1,Danny,1,1,par1");
    Map<String, String> record2 = Collections.singletonMap("id2", "id2,Betty,2,2,par1");

    try (HoodieJavaReadClient readClient = newReadClient()) {
      // a single-point range (both bounds inclusive, same value) selects exactly one commit's records,
      // with the correct field values (not just the right key set).
      assertEquals(record1, readAll(readClient.readIncremental(completion1, completion1)));
      assertEquals(record2, readAll(readClient.readIncremental(completion2, completion2)));

      // open-ended from c2's completion (inclusive) returns c2 + c3.
      Map<String, String> fromC2 = readAll(readClient.readIncremental(completion2, null));
      Map<String, String> expectedFromC2 = new HashMap<>();
      expectedFromC2.put("id2", "id2,Betty,2,2,par1");
      expectedFromC2.put("id3", "id3,Chris,3,3,par1");
      assertEquals(expectedFromC2, fromC2);

      // "earliest" with no end returns the full history.
      Map<String, String> all = readAll(readClient.readIncremental(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST, null));
      Map<String, String> expectedAll = new HashMap<>(expectedFromC2);
      expectedAll.put("id1", "id1,Danny,1,1,par1");
      assertEquals(expectedAll, all);

      // both bounds given (inclusive-inclusive) returns everything in between.
      Map<String, String> c1ToC2 = readAll(readClient.readIncremental(completion1, completion2));
      Map<String, String> expectedC1ToC2 = new HashMap<>();
      expectedC1ToC2.put("id1", "id1,Danny,1,1,par1");
      expectedC1ToC2.put("id2", "id2,Betty,2,2,par1");
      assertEquals(expectedC1ToC2, c1ToC2);
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  5. Incremental under NBCC out-of-order completion (the crown test)
  // -------------------------------------------------------------------------

  @Test
  public void testIncrementalUnderOutOfOrderCompletion() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());

    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);
    String instant1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> statuses1 = writeData(client1, instant1, "id1,Danny,1,1,par1", false);

    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    String instant2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> statuses2 = writeData(client2, instant2, "id2,Betty,2,2,par1", false);
    assertTrue(instant2.compareTo(instant1) > 0, "writer2 must have started after writer1");

    // out-of-order completion: the later-started writer2 commits first.
    commit(client2, instant2, statuses2);
    String completion2 = completionTimeOf(instant2);

    Map<String, String> record1 = Collections.singletonMap("id1", "id1,Danny,1,1,par1");
    Map<String, String> record2 = Collections.singletonMap("id2", "id2,Betty,2,2,par1");

    try (HoodieJavaReadClient readClient = newReadClient()) {
      // a range covering only writer2's completion time must return writer2's record only: writer1 is
      // still inflight and must not leak, even though it started (and would sort) earlier by requested time.
      assertEquals(record2, readAll(readClient.readIncremental(completion2, completion2)));
      // a wide historical range ending at writer2's completion must not leak writer1's (still inflight,
      // later-completing) data either.
      assertEquals(record2, readAll(readClient.readIncremental(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST, completion2)));
    }

    // the earlier-started writer1 completes last.
    commit(client1, instant1, statuses1);
    String completion1 = completionTimeOf(instant1);
    assertTrue(completion1.compareTo(completion2) > 0, "writer1 must have completed after writer2");

    try (HoodieJavaReadClient readClient = newReadClient()) {
      assertEquals(record1, readAll(readClient.readIncremental(completion1, completion1)));
      Map<String, String> both = new HashMap<>();
      both.putAll(record1);
      both.putAll(record2);
      assertEquals(both, readAll(readClient.readIncremental(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST, null)));
    }
    client1.close();
    client2.close();
  }

  // -------------------------------------------------------------------------
  //  6. Incremental skips the compaction instant
  // -------------------------------------------------------------------------

  @Test
  public void testIncrementalSkipsCompactionInstant() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1", true);
    writeAndCommit(client, "id1,Danny,11,2,par1", true, WriteOperationType.UPSERT);
    String compactionInstant = compact(client);
    String completionOfCompaction = completionTimeOf(compactionInstant);

    metaClient.reloadActiveTimeline();
    IncrementalQueryAnalyzer.QueryContext withSkip = IncrementalQueryAnalyzer.builder()
        .metaClient(metaClient)
        .startCompletionTime(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST)
        .endCompletionTime(completionOfCompaction)
        .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .skipCompaction(true)
        .build()
        .analyze();
    boolean compactionIncludedWithSkip = withSkip.getInstants().stream()
        .anyMatch(instant -> instant.getAction().equals(HoodieTimeline.COMMIT_ACTION));
    assertFalse(compactionIncludedWithSkip, "skipCompaction=true must exclude the compaction ('commit') instant from the range");

    IncrementalQueryAnalyzer.QueryContext withoutSkip = IncrementalQueryAnalyzer.builder()
        .metaClient(metaClient)
        .startCompletionTime(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST)
        .endCompletionTime(completionOfCompaction)
        .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .skipCompaction(false)
        .build()
        .analyze();
    boolean compactionIncludedWithoutSkip = withoutSkip.getInstants().stream()
        .anyMatch(instant -> instant.getAction().equals(HoodieTimeline.COMMIT_ACTION));
    assertTrue(compactionIncludedWithoutSkip, "skipCompaction=false must include the compaction instant in the range");

    // data-level sanity: default (skipCompaction=true) incremental read through the compaction's own
    // completion time (no writes happen after compaction here) returns id1's final merged value exactly
    // once, proving the two log blocks (insert + update) that compaction later physically merges don't ALSO
    // show up a second time as if the compaction commit re-emitted them as a new logical write. Collect into
    // a List (not the usual key-deduping readAll Map) so a genuine duplicate row would actually be caught.
    try (HoodieJavaReadClient readClient = newReadClient()) {
      List<String> keys = readAllRecordKeys(readClient.readIncremental(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST, completionOfCompaction));
      assertEquals(Collections.singletonList("id1"), keys, "id1 must appear exactly once, not duplicated by the compaction commit");
    }

    // gap coverage: IncrementalConfig must be honored through the public readIncremental(start, end, config)
    // entry point (not just when driving IncrementalQueryAnalyzer directly, as the assertions above do) --
    // skipCompaction=false must not throw and must not change the de-duplicated data-level result, since the
    // compaction instant carries no NEW logical data of its own (it's a physical rewrite of what the two log
    // blocks already contributed).
    try (HoodieJavaReadClient readClient = newReadClient()) {
      HoodieJavaReadClient.IncrementalConfig includeCompaction = HoodieJavaReadClient.IncrementalConfig.builder()
          .skipCompaction(false)
          .build();
      Map<String, String> incremental = readAll(readClient.readIncremental(
          IncrementalQueryAnalyzer.START_COMMIT_EARLIEST, completionOfCompaction, includeCompaction));
      assertEquals(Collections.singletonMap("id1", "id1,Danny,11,2,par1"), incremental,
          "IncrementalConfig.skipCompaction(false) passed through the public API must be honored without error");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  7. Time travel
  // -------------------------------------------------------------------------

  @Test
  public void testReadSnapshotTimeTravel() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    String c1 = writeAndCommit(client, "id1,Danny,1,1,par1", true);
    writeAndCommit(client, "id1,Danny,2,2,par1", true, WriteOperationType.UPSERT);
    writeAndCommit(client, "id1,Danny,3,3,par1", true, WriteOperationType.UPSERT);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> asOfC1 = readAll(readClient.readSnapshot(c1));
      assertEquals(Collections.singletonMap("id1", "id1,Danny,1,1,par1"), asOfC1,
          "time-travel read as of c1 must reflect only c1's value, ignoring the later commits");

      Map<String, String> latest = readAll(readClient.readSnapshot());
      assertEquals(Collections.singletonMap("id1", "id1,Danny,3,3,par1"), latest,
          "a plain snapshot read must reflect the latest value");
    }

    try (HoodieJavaReadClient readClient = newReadClient()) {
      assertThrows(IllegalArgumentException.class, () -> readClient.readSnapshot("00000000000000"),
          "time-travelling to a requested time that names no completed instant must fail loudly");
    }
    client.close();
  }

  @Test
  public void testTimeTravelRejectsScheduledButIncompleteCompactionInstant() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
    try {
      writeAndCommit(client, "id1,Danny,1,1,par1", true);
      writeAndCommit(client, "id1,Danny,11,2,par1", true, WriteOperationType.UPSERT);
      String scheduledCompaction = (String) client.scheduleCompaction(Option.empty()).get();
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getActiveTimeline().getInstantsAsStream()
          .anyMatch(instant -> instant.requestedTime().equals(scheduledCompaction)),
          "the compaction instant must exist on the active timeline for this regression to be meaningful");
      assertFalse(metaClient.getActiveTimeline().filterCompletedInstants().containsInstant(scheduledCompaction),
          "the compaction must remain scheduled, not completed");

      try (HoodieJavaReadClient readClient = newReadClient()) {
        assertThrows(IllegalArgumentException.class, () -> readClient.readSnapshot(scheduledCompaction),
            "time travel must reject a scheduled compaction because the API promises a completed target instant");
      }
    } finally {
      client.close();
    }
  }

  /**
   * Ports Spark's read-optimized regression for the stronger pending-compaction state: compact() has
   * produced a replacement base file but commitCompaction() has not made it visible. A later completed
   * delta commit must not make that inflight base file visible accidentally.
   */
  @Test
  public void testReadsIgnoreInflightCompactionBaseFileButSeeLaterDeltaCommit() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
    try {
      writeAndCommit(client, "id1,Danny,1,1,par1", true);
      writeAndCommit(client, "id1,Danny,11,2,par1", true, WriteOperationType.UPSERT);
      compact(client);

      writeAndCommit(client, "id1,Danny,22,3,par1", true, WriteOperationType.UPSERT);
      String inflightCompaction = (String) client.scheduleCompaction(Option.empty()).get();
      client.compact(inflightCompaction);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertFalse(metaClient.getActiveTimeline().filterCompletedInstants().containsInstant(inflightCompaction),
          "the replacement base file must remain inflight for this regression to be meaningful");

      writeAndCommit(client, "id1,Danny,33,4,par1", true, WriteOperationType.UPSERT);

      try (HoodieJavaReadClient readClient = newReadClient()) {
        assertEquals(Collections.singletonMap("id1", "id1,Danny,11,2,par1"),
            readAll(readClient.readOptimized()),
            "read-optimized must use the last completed base file, not the inflight compaction output");
        assertEquals(Collections.singletonMap("id1", "id1,Danny,33,4,par1"),
            readAll(readClient.readSnapshot()),
            "snapshot must merge through the later completed delta commit while compaction is inflight");
      }
    } finally {
      client.close();
    }
  }

  // -------------------------------------------------------------------------
  //  8. Copy-on-write sanity
  // -------------------------------------------------------------------------

  @Test
  public void testSnapshotAndIncrementalOnCopyOnWrite() throws Exception {
    // Unlike getPropertiesForKeyGen(true) (which hardcodes "_row_key"/"partition_path"), declare the
    // key/partition fields that actually exist in jsonSchema ("id"/"part") -- harmless for the bucket-index
    // NBCC tests above (a single bucket masks any key-gen field mismatch), but COW's write path is more
    // sensitive to it and needs it to be accurate.
    Properties props = new Properties();
    props.put(TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "part");
    props.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id");
    props.put(HoodieTableConfig.PARTITION_FIELDS.key(), "part");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .forTable("test")
        .withPath(basePath)
        .withSchema(jsonSchema)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadClass(OverwriteWithLatestAvroPayload.class.getName()).build())
        // Disable small-file merging: each insert below is a separate commit for a distinct key, and this
        // test's focus is snapshot/incremental correctness across file groups, not small-file handling.
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(HoodieIndex.IndexType.SIMPLE).build())
        .withPopulateMetaFields(true)
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    String c1 = writeAndCommit(client, "id1,Danny,1,1,par1", true);
    writeAndCommit(client, "id2,Betty,2,2,par1", true);
    String completion1 = completionTimeOf(c1);

    Map<String, String> expected = new HashMap<>();
    expected.put("id1", "id1,Danny,1,1,par1");
    expected.put("id2", "id2,Betty,2,2,par1");

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      assertEquals(expected, snapshot, "COW snapshot must see both inserted records");

      // On COW every file slice is base-file-only (no log files), so read-optimized must always agree with
      // the plain snapshot read.
      Map<String, String> readOptimized = readAll(readClient.readOptimized());
      assertEquals(expected, readOptimized, "COW readOptimized() must equal readSnapshot(): there are no log files");

      // [earliest, +INF] is the "latest snapshot" case: no per-record filtering, so the full-history
      // incremental read must agree with the plain snapshot read.
      Map<String, String> incremental = readAll(readClient.readIncremental(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST, null));
      assertEquals(expected, incremental, "HoodieJavaReadClient must not be MoR-only: COW incremental works too");

      // a real bounded range (not just the earliest/open-ended shortcut) must isolate a single commit's data.
      Map<String, String> boundedRange = readAll(readClient.readIncremental(completion1, completion1));
      assertEquals(Collections.singletonMap("id1", "id1,Danny,1,1,par1"), boundedRange,
          "COW incremental with a bounded range must isolate exactly the in-range commit's data");
    }
    client.close();
  }

  /**
   * Ports the engine-neutral invariant from Spark's schema-on-read/FGR coverage: a column rename is
   * identified by its InternalSchema field id, not by its Avro position or its old physical name.  The
   * data below remains in an old-schema MOR log block while ALTER_SCHEMA changes only the table schema,
   * so a latest-schema-only reader cannot satisfy this assertion accidentally by reading rewritten data.
   */
  @Test
  public void testSnapshotReconcilesInternalSchemaRenameInOldLogBlock() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
    try {
      writeAndCommit(client, "id1,Danny,1,1,par1", true);
      client.renameColumn("name", "full_name");
      metaClient = HoodieTableMetaClient.reload(metaClient);

      TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
      assertTrue(schemaResolver.getTableInternalSchemaFromCommitMetadata().isPresent(),
          "ALTER_SCHEMA must persist an InternalSchema for this regression to be meaningful");
      assertNotNull(schemaResolver.getTableInternalSchemaFromCommitMetadata().get().findField("full_name"));
      assertNull(schemaResolver.getTableInternalSchemaFromCommitMetadata().get().findField("name"));

      try (HoodieJavaReadClient readClient = newReadClient();
           ClosableIterator<HoodieRecord<IndexedRecord>> records = readClient.readSnapshot()) {
        assertTrue(records.hasNext(), "the old-schema log block must remain readable after the rename");
        GenericRecord record = (GenericRecord) records.next().getData();
        assertNotNull(record.getSchema().getField("full_name"),
            "the returned record must use the evolved schema");
        assertEquals("Danny", String.valueOf(record.get("full_name")),
            "InternalSchema must carry the old 'name' value into the renamed 'full_name' field");
        assertFalse(records.hasNext());
      }
    } finally {
      client.close();
    }
  }

  /**
   * Mirrors Flink's direct-client CDC-enabled MOR write coverage at the engine-neutral boundary. MOR does
   * not need a separate CDC log for these writes: HoodieCDCExtractor must be able to plan their changes from
   * the regular Java-engine log files using the LOG_FILE inference case.
   */
  @ParameterizedTest
  @CsvSource({"OP_KEY_ONLY,false", "OP_KEY_ONLY,true", "DATA_BEFORE,false", "DATA_BEFORE,true",
      "DATA_BEFORE_AFTER,false", "DATA_BEFORE_AFTER,true"})
  public void testCdcEnabledMorWritesAreConsumableByCommonCdcExtractor(String loggingMode, boolean compactFirst) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    config.getProps().setProperty(HoodieTableConfig.CDC_ENABLED.key(), Boolean.TRUE.toString());
    config.getProps().setProperty(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key(), loggingMode);
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
    try {
      String insertInstant = writeAndCommit(client, "id1,Danny,1,1,par1", true);
      if (compactFirst) {
        compact(client);
      }
      String duplicateInstant = writeAndCommit(client, "id1,Danny,1,1,par1", true);
      try (HoodieJavaReadClient reader = newReadClient()) {
        assertEquals(Collections.singletonList("id1"), readAllRecordKeys(reader.readSnapshot()),
            "CDC-enabled repeated inserts must merge into one visible key");
      }
      String updateInstant = writeAndCommit(client, "id1,Danny,2,2,par1", true, WriteOperationType.UPSERT);
      try (HoodieJavaReadClient reader = newReadClient()) {
        assertEquals(Collections.singletonMap("id1", "id1,Danny,2,2,par1"), readAll(reader.readSnapshot()));
      }
      String deleteInstant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, deleteInstant);
      List<WriteStatus> deleted = client.delete(new ArrayList<>(Collections.singletonList(new HoodieKey("id1", "par1"))), deleteInstant);
      assertNoErrors(deleted);
      commit(client, deleteInstant, deleted);
      try (HoodieJavaReadClient reader = newReadClient()) {
        assertTrue(readAllRecordKeys(reader.readSnapshot()).isEmpty());
      }
      String reinsertInstant = writeAndCommit(client, "id1,returned,3,3,par1", true);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getTableConfig().isCDCEnabled(),
          "the table must be CDC-enabled for this interoperability test to be meaningful");

      Set<String> expectedInstants = new HashSet<>();
      expectedInstants.add(insertInstant);
      expectedInstants.add(duplicateInstant);
      expectedInstants.add(updateInstant);
      expectedInstants.add(deleteInstant);
      expectedInstants.add(reinsertInstant);
      InstantRange range = InstantRange.builder()
          .rangeType(InstantRange.RangeType.EXACT_MATCH)
          .explicitInstants(expectedInstants)
          .build();
      Map<HoodieFileGroupId, List<HoodieCDCFileSplit>> byFileGroup =
          new HoodieCDCExtractor(metaClient, range, false).extractCDCFileSplits();
      List<HoodieCDCFileSplit> splits = byFileGroup.values().stream()
          .flatMap(List::stream)
          .collect(Collectors.toList());

      assertEquals(expectedInstants,
          splits.stream().map(HoodieCDCFileSplit::getInstant).collect(Collectors.toSet()),
          "the common CDC planner must retain inserts, repeated inserts, updates, deletes and reinserts");
      assertTrue(splits.stream().allMatch(split -> split.getCdcInferCase() == HoodieCDCInferenceCase.LOG_FILE),
          "Java MOR changes should be inferred from their normal log files when no separate CDC log exists");
      assertTrue(splits.stream().allMatch(split -> !split.getCdcFiles().isEmpty()),
          "every planned CDC change must identify its source log file");
      compact(client);
      try (HoodieJavaReadClient reader = newReadClient()) {
        assertEquals(Collections.singletonMap("id1", "id1,returned,3,3,par1"), readAll(reader.readOptimized()));
        assertEquals(Collections.singletonList("id1"), readAllRecordKeys(reader.readSnapshot()));
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testUnsupportedReadConfigurationsAreRejectedAtEntryPoint() throws Exception {
    Properties props = new Properties();
    props.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "part");
    props.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id");
    props.put(HoodieTableConfig.PARTITION_FIELDS.key(), "part");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .forTable("test")
        .withPath(basePath)
        .withSchema(jsonSchema)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder()
            .withPayloadClass(OverwriteWithLatestAvroPayload.class.getName()).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.SIMPLE)
            .build())
        .withPopulateMetaFields(false)
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
    try {
      String commit = writeAndCommit(client, "id1,Danny,1,1,par1", true);
      String completionTime = completionTimeOf(commit);

      try (HoodieJavaReadClient readClient = newReadClient()) {
        assertAll(
            () -> assertThrows(UnsupportedOperationException.class,
                () -> readClient.readIncremental(completionTime, completionTime),
                "the documented no-meta-fields limitation must be checked before returning a lazy iterator"),
            this::assertLsmStorageLayoutIsRejectedAtEntryPoint);
      }
    } finally {
      client.close();
    }
  }

  private void assertLsmStorageLayoutIsRejectedAtEntryPoint() throws IOException {
    Properties props = new Properties();
    props.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    props.put(HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(),
        HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue());
    HoodieTableMetaClient lsmMetaClient = HoodieTestUtils.init(
        storageConf, basePath + "_lsm", HoodieTableType.MERGE_ON_READ, props);
    assertTrue(lsmMetaClient.getTableConfig().isLSMTreeStorageLayout(),
        "the table must use LSM storage for this regression to be meaningful");

    try (HoodieJavaReadClient readClient = new HoodieJavaReadClient(context, lsmMetaClient, new TypedProperties())) {
      assertThrows(UnsupportedOperationException.class, readClient::readSnapshot,
          "v1 must explicitly reject LSM storage until it selects HoodieLsmFileGroupReader for merged reads");
    }
  }

  // -------------------------------------------------------------------------
  //  9. Finding 1: snapshot must include records written after compaction is SCHEDULED but not yet run
  // -------------------------------------------------------------------------

  @Test
  public void testSnapshotIncludesRecordsWrittenAfterCompactionScheduled() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1", true);
    writeAndCommit(client, "id1,Danny,11,2,par1", true, WriteOperationType.UPSERT);

    // schedule (but do NOT run) a compaction: bucket 0's file group now has a pending compaction plan whose
    // instant time becomes the base instant for any FUTURE file slice on that file group.
    String compactionInstant = (String) client.scheduleCompaction(Option.empty()).get();
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // write again to the same bucket: this new log file's file slice has the pending compaction's instant
    // as its base instant. Before the fix (filterCompletedInstants instead of
    // filterCompletedAndCompactionInstants), HoodieFileGroup#isFileSliceCommitted would reject this slice
    // outright since the timeline used to build the view didn't even recognize the pending compaction
    // instant, silently dropping id1's latest value from the snapshot.
    writeAndCommit(client, "id1,Danny,111,3,par1", true, WriteOperationType.UPSERT);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      assertEquals(Collections.singletonMap("id1", "id1,Danny,111,3,par1"), snapshot,
          "snapshot must include records written after a compaction is scheduled but before it runs");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  10. Finding 2: [earliest, boundedEnd] must stay completion-time-true under NBCC out-of-order completion
  // -------------------------------------------------------------------------

  @Test
  public void testIncrementalEarliestBoundedRangeExcludesLateCompletingWriter() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());

    // writer1 has an EARLY requested time (starts first).
    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);
    String instant1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> statuses1 = writeData(client1, instant1, "id1,Danny,1,1,par1", false);

    // writer2 has a LATER requested time (starts second).
    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    String instant2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> statuses2 = writeData(client2, instant2, "id2,Betty,2,2,par1", false);
    assertTrue(instant2.compareTo(instant1) > 0, "writer2 must have a later requested time than writer1");

    // out-of-order completion: writer2 (later requested time) completes FIRST.
    commit(client2, instant2, statuses2);
    String completion2 = completionTimeOf(instant2);

    // writer1 (earlier requested time) completes LAST, and this read is issued AFTER that completion --
    // writer1 is fully committed by the time we read, just with a completion time after completion2.
    commit(client1, instant1, statuses1);
    String completion1 = completionTimeOf(instant1);
    assertTrue(completion1.compareTo(completion2) > 0, "writer1 must have completed after writer2");

    try (HoodieJavaReadClient readClient = newReadClient()) {
      // [earliest, completion2]: writer1's own completion (completion1) is AFTER this range's end bound, so
      // its record must be absent even though its requested time is earlier than writer2's -- a
      // requested-time-based filter would incorrectly let it leak in here.
      Map<String, String> result = readAll(readClient.readIncremental(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST, completion2));
      assertEquals(Collections.singletonMap("id2", "id2,Betty,2,2,par1"), result,
          "writer1's late-completing record must not leak into a completion-time range that ended before it completed");
    }
    client1.close();
    client2.close();
  }

  // -------------------------------------------------------------------------
  //  11. Finding 4: incremental reads must restrict to partitions touched by in-range commits
  // -------------------------------------------------------------------------

  @Test
  public void testIncrementalRestrictsToTouchedPartitions() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    String c1 = writeAndCommit(client, "id1,Danny,1,1,par1", true);
    String completion1 = completionTimeOf(c1);
    // par2 is written AFTER c1's range, so a query bounded to c1 must never touch it.
    writeAndCommit(client, "id2,Betty,2,2,par2", true);

    IncrementalQueryAnalyzer.QueryContext queryContext = IncrementalQueryAnalyzer.builder()
        .metaClient(metaClient)
        .startCompletionTime(completion1)
        .endCompletionTime(completion1)
        .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .build()
        .analyze();

    try (HoodieJavaReadClient readClient = newReadClient()) {
      List<String> partitions = readClient.partitionsForIncrementalRead(queryContext);
      assertEquals(Collections.singletonList("par1"), partitions,
          "only par1 (touched by the in-range commit) may be scanned; par2 was written outside the range");

      // data-level corroboration: the actual read result matches too.
      Map<String, String> result = readAll(readClient.readIncremental(completion1, completion1));
      assertEquals(Collections.singletonMap("id1", "id1,Danny,1,1,par1"), result);
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  12. Gap (b): incremental with a START bound after compaction, EXACT_MATCH applied to a BASE FILE record
  // -------------------------------------------------------------------------

  @Test
  public void testIncrementalStartBoundAfterCompactionAppliesToBaseFile() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1", true);
    writeAndCommit(client, "id1,Danny,11,2,par1", true, WriteOperationType.UPSERT);
    compact(client);

    // bulk_insert into a DIFFERENT partition (par2) after the compaction: bulk_insert always writes a base
    // file directly, so the EXACT_MATCH InstantRange built for a start-bound-after-compaction range gets
    // applied to a record physically stored in a base file, not a log block -- the core incremental-on-MoR
    // path this gap was about.
    String bulkInsertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> bulkInsertRecords = Collections.singletonList(str2HoodieRecord("id2,Betty,2,3,par2"));
    WriteClientTestUtils.startCommitWithTime(client, bulkInsertTime);
    List<WriteStatus> bulkInsertStatuses = client.bulkInsert(bulkInsertRecords, bulkInsertTime);
    assertNoErrors(bulkInsertStatuses);
    commit(client, bulkInsertTime, bulkInsertStatuses);
    String completionBulkInsert = completionTimeOf(bulkInsertTime);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> result = readAll(readClient.readIncremental(completionBulkInsert, completionBulkInsert));
      assertEquals(Collections.singletonMap("id2", "id2,Betty,2,3,par2"), result,
          "instant-range filtering on a base-file record, with a start bound after a compaction, must work");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  13. Gap (c): documented null-start semantics (single latest instant, not "from the beginning")
  // -------------------------------------------------------------------------

  @Test
  public void testReadIncrementalNullStartSemantics() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1", true);
    String c2 = writeAndCommit(client, "id2,Betty,2,2,par1", true);
    writeAndCommit(client, "id3,Chris,3,3,par1", true);
    String completion2 = completionTimeOf(c2);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      // startCompletionTime == null, endCompletionTime == null: per the documented "usual streaming read
      // semantics" default, this is NOT "from the beginning" -- only the single latest completed instant.
      Map<String, String> nullNull = readAll(readClient.readIncremental(null, null));
      assertEquals(Collections.singletonMap("id3", "id3,Chris,3,3,par1"), nullNull,
          "readIncremental(null, null) must return only the single latest completed instant, not full history");

      // startCompletionTime == null with a bounded end: still collapses to the single (last) instant that
      // satisfies the end bound, not everything up to it.
      Map<String, String> nullBounded = readAll(readClient.readIncremental(null, completion2));
      assertEquals(Collections.singletonMap("id2", "id2,Betty,2,2,par1"), nullBounded,
          "readIncremental(null, boundedEnd) must return only the single latest in-range instant");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  14. Gap (c): empty table for all three read methods
  // -------------------------------------------------------------------------

  @Test
  public void testEmptyTableReadsAreEmpty() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());

    try (HoodieJavaReadClient readClient = newReadClient()) {
      assertTrue(readAll(readClient.readSnapshot()).isEmpty(), "readSnapshot() on an empty table must be empty");
      assertTrue(readAll(readClient.readOptimized()).isEmpty(), "readOptimized() on an empty table must be empty");
      assertTrue(readAll(readClient.readIncremental(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST, null)).isEmpty(),
          "readIncremental() on an empty table must be empty");
    }
  }

  // -------------------------------------------------------------------------
  //  15. Gap (c): a completion-time range falling into the archived timeline throws
  // -------------------------------------------------------------------------

  @Test
  public void testIncrementalArchivedRangeThrowsUnsupported() throws Exception {
    Properties props = getPropertiesForKeyGen(true);
    props.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .forTable("test")
        .withPath(basePath)
        .withSchema(jsonSchema)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadClass(OverwriteWithLatestAvroPayload.class.getName()).build())
        // Aggressive archival: keep as few commits active as possible so a handful of commits is enough to
        // push the earliest ones into the archived timeline. Archival only considers commits already safe
        // to clean up, so auto-clean (default) must stay enabled with an equally aggressive retention.
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(1, 2).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(HoodieIndex.IndexType.SIMPLE).build())
        .withPopulateMetaFields(true)
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    String c1 = writeAndCommit(client, "id1,Danny,1,1,par1", true);
    String completion1 = completionTimeOf(c1);
    writeAndCommit(client, "id2,Betty,2,2,par1", true);
    writeAndCommit(client, "id3,Chris,3,3,par1", true);
    writeAndCommit(client, "id4,Dana,4,4,par1", true);
    writeAndCommit(client, "id5,Eve,5,5,par1", true);
    writeAndCommit(client, "id6,Frank,6,6,par1", true);
    writeAndCommit(client, "id7,Grace,7,7,par1", true);
    client.clean();
    client.archive();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(metaClient.getActiveTimeline().filterCompletedInstants().containsInstant(c1),
        "c1 must actually have been archived off the active timeline for this test to be meaningful");

    try (HoodieJavaReadClient readClient = newReadClient()) {
      assertThrows(UnsupportedOperationException.class, () -> readClient.readIncremental(completion1, completion1),
          "a range falling into the archived timeline must throw, not silently return wrong/partial data");
      assertThrows(IllegalArgumentException.class, () -> readClient.readSnapshot(c1),
          "time travel currently supports active-timeline instants only and must reject an archived target");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  16. Gap (c): partial iteration followed by close() releases resources cleanly
  // -------------------------------------------------------------------------

  @Test
  public void testPartialIterationThenCloseReleasesResources() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1", true);
    writeAndCommit(client, "id2,Betty,2,2,par2", true);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      ClosableIterator<HoodieRecord<IndexedRecord>> iterator = readClient.readSnapshot();
      assertTrue(iterator.hasNext());
      iterator.next();
      // close without exhausting the iterator -- must not throw, and must not leave anything in a state
      // that breaks a subsequent, independent read.
      iterator.close();

      Map<String, String> secondRead = readAll(readClient.readSnapshot());
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,Danny,1,1,par1");
      expected.put("id2", "id2,Betty,2,2,par2");
      assertEquals(expected, secondRead, "a subsequent independent read must work fine after an earlier partial iteration was closed early");
    }
    client.close();
  }

  /**
   * Ports Spark's MOR spill test through the standalone reader.  This proves the bounded merge-buffer
   * mechanism, including cleanup, rather than relying on a process-wide heap measurement that would be
   * unstable in CI.
   */
  @ParameterizedTest
  @CsvSource({"BITCASK,false,false", "BITCASK,false,true", "BITCASK,true,false", "BITCASK,true,true",
      "ROCKS_DB,false,false", "ROCKS_DB,false,true", "ROCKS_DB,true,false", "ROCKS_DB,true,true"})
  public void testSnapshotSpillsMergeBufferAndCleansItOnClose(String diskMapType, boolean compactFirst,
                                                            boolean closeEarly) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
    try {
      List<HoodieRecord> inserts = new ArrayList<>();
      List<HoodieRecord> updates = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        inserts.add(str2HoodieRecord("id" + i + ",name" + i + "," + i + ",1,par1"));
        updates.add(str2HoodieRecord("id" + i + ",updated" + i + "," + (i + 100) + ",2,par1"));
      }
      String insertInstant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, insertInstant);
      List<WriteStatus> insertStatuses = client.insert(inserts, insertInstant);
      assertNoErrors(insertStatuses);
      commit(client, insertInstant, insertStatuses);

      if (compactFirst) {
        compact(client);
      }

      String updateInstant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, updateInstant);
      List<WriteStatus> updateStatuses = client.upsert(updates, updateInstant);
      assertNoErrors(updateStatuses);
      commit(client, updateInstant, updateStatuses);

      Path spillDirectory = tempDir.resolve("java-read-client-spill");
      Files.createDirectories(spillDirectory);
      TypedProperties readProps = new TypedProperties();
      readProps.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), "1");
      readProps.setProperty(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), diskMapType);
      readProps.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), spillDirectory.toString());

      try (HoodieJavaReadClient readClient = new HoodieJavaReadClient(context, metaClient, readProps)) {
        try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator = readClient.readSnapshot()) {
          assertTrue(iterator.hasNext(), "opening the first file group must expose the merged records");
          try (Stream<Path> spillFiles = Files.list(spillDirectory)) {
            assertTrue(spillFiles.findAny().isPresent(),
                "a one-byte merge budget must force the file-group buffer to disk");
          }

          if (closeEarly) {
            assertNotNull(iterator.next());
          } else {
            Map<String, String> expected = new HashMap<>();
            for (int i = 0; i < 100; i++) {
              expected.put("id" + i, "id" + i + ",updated" + i + "," + (i + 100) + ",2,par1");
            }
            assertEquals(expected, readAll(iterator));
          }
        }
      }

      try (Stream<Path> spillFiles = Files.list(spillDirectory)) {
        assertFalse(spillFiles.findAny().isPresent(),
            "closing the returned iterator must remove its spill-map directory");
      }
    } finally {
      client.close();
    }
  }

  // -------------------------------------------------------------------------
  //  17. Gap (c): a delete record is absent (not emitted) under incremental read (emitDelete=false)
  // -------------------------------------------------------------------------

  @Test
  public void testDeletedRecordAbsentFromIncrementalRead() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1", true);

    String deleteInstant = WriteClientTestUtils.createNewInstantTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    WriteClientTestUtils.startCommitWithTime(client, deleteInstant);
    List<HoodieKey> keysToDelete = new ArrayList<>();
    keysToDelete.add(new HoodieKey("id1", "par1"));
    List<WriteStatus> deleteStatuses = client.delete(keysToDelete, deleteInstant);
    assertNoErrors(deleteStatuses);
    commit(client, deleteInstant, deleteStatuses);
    String completionDelete = completionTimeOf(deleteInstant);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      // HoodieJavaReadClient never sets emitDelete=true (see Known limitations / FileGroupReader defaults),
      // so the delete tombstone itself must be ABSENT from incremental results, not emitted as some
      // "deleted" marker record.
      Map<String, String> incremental = readAll(readClient.readIncremental(completionDelete, completionDelete));
      assertTrue(incremental.isEmpty(), "a delete record must be absent under emitDelete=false, not emitted");

      // and the deleted key must also be gone from a plain snapshot read.
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      assertFalse(snapshot.containsKey("id1"), "a deleted key must not appear in a snapshot read");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  18. Gap (d): a write to the SAME file group AFTER compaction is visible via readSnapshot()
  // -------------------------------------------------------------------------

  @Test
  public void testSnapshotSeesWriteToSameFileGroupAfterCompaction() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1", true);
    writeAndCommit(client, "id1,Danny,11,2,par1", true, WriteOperationType.UPSERT);
    compact(client);

    // a NEW key into the SAME bucket (numBuckets=1), whose only file group now has a compacted base file:
    // routed as a log append over that base file.
    writeAndCommit(client, "id2,Betty,2,3,par1", true);
    // and a further UPDATE to id1 itself, also appended as a log entry over the same compacted base file.
    writeAndCommit(client, "id1,Danny,111,4,par1", true, WriteOperationType.UPSERT);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,Danny,111,4,par1");
      expected.put("id2", "id2,Betty,2,3,par1");
      assertEquals(expected, snapshot,
          "snapshot must correctly reflect both a new key and an update written to the same file group after compaction");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  19. Gap (d): a write to the SAME file group AFTER compaction is visible via readIncremental()
  // -------------------------------------------------------------------------

  @Test
  public void testIncrementalSeesWriteToSameFileGroupAfterCompaction() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1", true);
    writeAndCommit(client, "id1,Danny,11,2,par1", true, WriteOperationType.UPSERT);
    compact(client);

    // an UPDATE to id1, appended as a log entry over the just-compacted base file.
    String c3 = writeAndCommit(client, "id1,Danny,111,3,par1", true, WriteOperationType.UPSERT);
    String completion3 = completionTimeOf(c3);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> result = readAll(readClient.readIncremental(completion3, completion3));
      assertEquals(Collections.singletonMap("id1", "id1,Danny,111,3,par1"), result,
          "incremental read must correctly reflect an update written to the same file group after compaction");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  Helpers
  // -------------------------------------------------------------------------

  private HoodieJavaReadClient newReadClient() {
    return new HoodieJavaReadClient(context, metaClient, new TypedProperties());
  }

  /** Spark MOR rollback parity: undo both new keys and updates, with either rollback discovery mode. */
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  public void testRollbackRestoresExactSnapshot(boolean compactFirst, boolean rollbackUsingMarkers) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withRollbackUsingMarkers(rollbackUsingMarkers).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      writeAndCommit(client, "id2,untouched,2,1,par2", true);
      Map<String, String> expected = readAll(reader.readSnapshot());
      if (compactFirst) {
        compact(client);
      }
      // Completed instants always use listing-based rollback. An inflight write is necessary to
      // exercise the marker-based strategy when rollbackUsingMarkers is true.
      String abandoned = writeAndCommit(client, "id1,abandoned,8,2,par1", false, WriteOperationType.UPSERT);
      assertTrue(metaClient.reloadActiveTimeline().getCommitsTimeline().filterInflights().containsInstant(abandoned));
      assertTrue(client.rollback(abandoned));
      assertEquals(expected, readAll(reader.readSnapshot()), "rolling back an inflight append must preserve committed blocks");
      String update = writeAndCommit(client, "id1,updated,9,2,par1", true, WriteOperationType.UPSERT);
      String insert = writeAndCommit(client, "id3,new,3,3,par1", true);
      assertEquals(3, readAllRecordKeys(reader.readSnapshot()).size());
      assertTrue(client.rollback(insert));
      assertTrue(client.rollback(update));
      assertEquals(expected, readAll(reader.readSnapshot()), "rollback must restore values as well as row count");
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
      assertTrue(readAll(reader.readIncremental(completionTimeOfLatestRollback(), null)).isEmpty(),
          "rollback must not surface as incremental data");
      assertFalse(metaClient.reloadActiveTimeline().getCommitsTimeline().containsInstant(update));
      assertFalse(metaClient.getActiveTimeline().getCommitsTimeline().containsInstant(insert));
      writeAndCommit(client, "id1,afterRollback,10,4,par1", true, WriteOperationType.UPSERT);
      expected.put("id1", "id1,afterRollback,10,4,par1");
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()), "writing after rollback must remain possible");
    }
  }

  private String completionTimeOfLatestRollback() {
    return metaClient.reloadActiveTimeline().getRollbackTimeline().filterCompletedInstants()
        .lastInstant().get().getCompletionTime();
  }

  /** Spark savepoint/restore parity across a later compaction and an abandoned delta write. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSavepointRestoreAcrossCompaction(boolean compactBeforeSavepoint) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      String savepoint = writeAndCommit(client, "id1,saved,1,1,par1", true);
      if (compactBeforeSavepoint) {
        savepoint = compact(client);
      }
      Map<String, String> expected = Collections.singletonMap("id1", "id1,saved,1,1,par1");
      client.savepoint(savepoint, "java-parity", "restore MOR base and log state");
      writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
      String laterCompaction = compact(client);
      writeAndCommit(client, "id2,new,3,3,par2", true);
      writeAndCommit(client, "id1,abandoned,4,4,par1", false, WriteOperationType.UPSERT);
      client.restoreToSavepoint(savepoint);
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(Collections.singletonList("id1"), readAllRecordKeys(reader.readSnapshot()));
      assertFalse(metaClient.reloadActiveTimeline().getCommitsTimeline().containsInstant(laterCompaction));
      assertTrue(metaClient.getActiveTimeline().getCommitsTimeline().filterInflights().empty());
      writeAndCommit(client, "id1,restored,5,5,par1", true, WriteOperationType.UPSERT);
      compact(client);
      assertEquals(Collections.singletonMap("id1", "id1,restored,5,5,par1"), readAll(reader.readOptimized()));
    }
  }

  /** Flink base/log delete parity, including repeated missing-key deletes and resurrection. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testDeleteAllAndReinsertAcrossCompaction(boolean compactFirst) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      if (compactFirst) {
        compact(client);
      }
      for (int attempt = 0; attempt < 2; attempt++) {
        String instant = WriteClientTestUtils.createNewInstantTime();
        WriteClientTestUtils.startCommitWithTime(client, instant);
        List<HoodieKey> keys = new ArrayList<>();
        keys.add(new HoodieKey("id1", "par1"));
        keys.add(new HoodieKey("missing", "par1"));
        List<WriteStatus> statuses = client.delete(keys, instant);
        assertNoErrors(statuses);
        commit(client, instant, statuses);
        assertTrue(readAllRecordKeys(reader.readSnapshot()).isEmpty());
      }
      compact(client);
      assertTrue(readAllRecordKeys(reader.readOptimized()).isEmpty());
      String reinsert = writeAndCommit(client, "id1,reborn,3,3,par1", true, WriteOperationType.UPSERT);
      Map<String, String> expected = Collections.singletonMap("id1", "id1,reborn,3,3,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(expected, readAll(reader.readIncremental(completionTimeOf(reinsert), completionTimeOf(reinsert))));
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(Collections.singletonList("id1"), readAllRecordKeys(reader.readSnapshot()));
    }
  }

  /** A lazy iterator must retain its own timeline even when the same client plans a newer read. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testOverlappingSnapshotIteratorsRetainTheirTimeline(boolean compactFirst) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      if (compactFirst) {
        compact(client);
      }
      try (ClosableIterator<HoodieRecord<IndexedRecord>> oldSnapshot = reader.readSnapshot()) {
        writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
        writeAndCommit(client, "id2,new,3,3,par2", true);
        Map<String, String> expected = new HashMap<>();
        expected.put("id1", "id1,updated,2,2,par1");
        expected.put("id2", "id2,new,3,3,par2");
        assertEquals(expected, readAll(reader.readSnapshot()));
        assertEquals(Collections.singletonMap("id1", "id1,original,1,1,par1"), readAll(oldSnapshot));
      }
    }
  }

  /** Exercise every remaining insert entry point followed by prepped/non-prepped updates and deletes. */
  @ParameterizedTest
  @CsvSource({"INSERT,false", "INSERT,true", "UPSERT,false", "UPSERT,true",
      "UPSERT_PREPPED,false", "UPSERT_PREPPED,true", "BULK_INSERT,false", "BULK_INSERT,true",
      "BULK_INSERT_PREPPED,false", "BULK_INSERT_PREPPED,true"})
  public void testWriteOperationLifecycle(WriteOperationType initialOperation, boolean compactBeforeUpdate) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    boolean prepped = initialOperation.name().endsWith("_PREPPED");
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      List<HoodieRecord> inserts = new ArrayList<>();
      inserts.add(str2HoodieRecord("id1,original,1,1,par1"));
      inserts.add(str2HoodieRecord("id2,untouched,2,1,par2"));
      String insert = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, insert);
      if (prepped) {
        inserts = tagLocation(JavaHoodieIndexFactory.createIndex(config), context, inserts,
            HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(metaClient)));
      }
      List<WriteStatus> statuses;
      switch (initialOperation) {
        case INSERT:
          statuses = client.insert(inserts, insert);
          break;
        case UPSERT:
          statuses = client.upsert(inserts, insert);
          break;
        case UPSERT_PREPPED:
          statuses = client.upsertPreppedRecords(inserts, insert);
          break;
        case BULK_INSERT:
          statuses = client.bulkInsert(inserts, insert);
          break;
        case BULK_INSERT_PREPPED:
          statuses = client.bulkInsertPreppedRecords(inserts, insert, Option.empty());
          break;
        default:
          throw new IllegalArgumentException("Unexpected operation " + initialOperation);
      }
      assertFalse(statuses.isEmpty());
      assertNoErrors(statuses);
      commit(client, insert, statuses);
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,original,1,1,par1");
      expected.put("id2", "id2,untouched,2,1,par2");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
      // Bulk insert and the insert branch of prepped upsert may already have produced base files.
      if (compactBeforeUpdate) {
        if (readAll(reader.readOptimized()).isEmpty()) {
          compact(client);
        } else {
          assertEquals(expected, readAll(reader.readOptimized()));
        }
      }
      List<HoodieRecord> updates = Collections.singletonList(str2HoodieRecord("id1,updated,9,2,par1"));
      if (prepped) {
        updates = tagLocation(JavaHoodieIndexFactory.createIndex(config), context, updates,
            HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(metaClient)));
        assertTrue(updates.get(0).isCurrentLocationKnown(), "prepped update must use the persisted record location");
      }
      String update = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, update);
      statuses = prepped ? client.upsertPreppedRecords(updates, update) : client.upsert(updates, update);
      assertNoErrors(statuses);
      assertTrue(metaClient.reloadActiveTimeline().getCommitsTimeline().filterInflights().containsInstant(update),
          "an update-only prepped write must transition the requested delta commit to inflight before commit");
      commit(client, update, statuses);
      expected.put("id1", "id1,updated,9,2,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(Collections.singletonMap("id1", "id1,updated,9,2,par1"),
          readAll(reader.readIncremental(completionTimeOf(update), completionTimeOf(update))));
      List<HoodieRecord> deletes = Collections.singletonList(new HoodieAvroRecord<>(
          new HoodieKey("id1", "par1"), new OverwriteWithLatestAvroPayload(Option.empty())));
      if (prepped) {
        deletes = tagLocation(JavaHoodieIndexFactory.createIndex(config), context, deletes,
            HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(metaClient)));
        assertTrue(deletes.get(0).isCurrentLocationKnown());
      }
      String delete = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, delete);
      statuses = prepped ? client.deletePrepped(deletes, delete)
          : client.delete(new ArrayList<>(Collections.singletonList(new HoodieKey("id1", "par1"))), delete);
      assertNoErrors(statuses);
      commit(client, delete, statuses);
      expected.remove("id1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(Collections.singletonList("id2"), readAllRecordKeys(reader.readSnapshot()));
    }
  }

  /** Keep prepped delete coverage independent of failures in the prepped upsert path. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testPreppedDeletePreservesOtherKeys(boolean compactFirst) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,delete,1,1,par1", true);
      writeAndCommit(client, "id2,keep,2,1,par1", true);
      if (compactFirst) {
        compact(client);
      }
      List<HoodieRecord> deletes = Collections.singletonList(new HoodieAvroRecord<>(
          new HoodieKey("id1", "par1"), new OverwriteWithLatestAvroPayload(Option.empty())));
      deletes = tagLocation(JavaHoodieIndexFactory.createIndex(config), context, deletes,
          HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(metaClient)));
      assertTrue(deletes.get(0).isCurrentLocationKnown());
      String instant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instant);
      List<WriteStatus> statuses = client.deletePrepped(deletes, instant);
      assertNoErrors(statuses);
      commit(client, instant, statuses);
      Map<String, String> expected = Collections.singletonMap("id2", "id2,keep,2,1,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertTrue(readAllRecordKeys(reader.readIncremental(completionTimeOf(instant), completionTimeOf(instant))).isEmpty());
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(Collections.singletonList("id2"), readAllRecordKeys(reader.readSnapshot()));
    }
  }

  /** Flink small-log-block parity: force rollover and verify every value across many blocks and partitions. */
  @Test
  public void testSmallLogBlocksAndRolloverPreserveAllRecords() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withStorageConfig(HoodieStorageConfig.newBuilder().logFileMaxSize(1024).logFileDataBlockMaxSize(512).build())
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      Map<String, String> expected = new HashMap<>();
      Set<String> logPaths = new HashSet<>();
      for (int batch = 0; batch < 6; batch++) {
        List<HoodieRecord> records = new ArrayList<>();
        for (int id = 0; id < 120; id++) {
          String value = "id" + id + ",batch" + batch + "," + (id + batch) + "," + batch + ",par" + (id % 4);
          records.add(str2HoodieRecord(value));
          expected.put("id" + id, value);
        }
        String instant = WriteClientTestUtils.createNewInstantTime();
        WriteClientTestUtils.startCommitWithTime(client, instant);
        List<WriteStatus> statuses = client.upsert(records, instant);
        assertNoErrors(statuses);
        statuses.forEach(status -> logPaths.add(status.getStat().getPath()));
        commit(client, instant, statuses);
      }
      assertTrue(logPaths.size() > 4, "fixture must roll over logs, not merely create one log per partition");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(120, readAllRecordKeys(reader.readSnapshot()).size(), "multiple blocks must not duplicate logical rows");
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(120, readAllRecordKeys(reader.readSnapshot()).size());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testEventTimeDisorderedDeletesAndResurrection(boolean compactFirst) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig(RecordMergeMode.EVENT_TIME_ORDERING, EventTimeAvroPayload.class.getName());
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeEventTimeAndCommit(client, "id1,winner,4,4,par1", WriteOperationType.INSERT);
      if (compactFirst) {
        compact(client);
      }
      writeEventTimeDelete(client, 2L);
      assertEquals(Collections.singletonMap("id1", "id1,winner,4,4,par1"), readAll(reader.readSnapshot()),
          "a late-arriving delete with an older ordering value must not remove the winner");
      writeEventTimeDelete(client, 6L);
      assertTrue(readAllRecordKeys(reader.readSnapshot()).isEmpty());
      writeEventTimeAndCommit(client, "id1,stale,5,5,par1", WriteOperationType.UPSERT);
      assertTrue(readAllRecordKeys(reader.readSnapshot()).isEmpty(), "stale updates must not resurrect a newer log tombstone");
      writeEventTimeAndCommit(client, "id1,reborn,7,7,par1", WriteOperationType.UPSERT);
      Map<String, String> expected = Collections.singletonMap("id1", "id1,reborn,7,7,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
    }
  }

  private void writeEventTimeDelete(HoodieJavaWriteClient client, long orderingValue) throws IOException {
    String instant = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instant);
    HoodieRecord tombstone = new HoodieAvroRecord<>(new HoodieKey("id1", "par1"),
        new EventTimeAvroPayload(null, orderingValue));
    List<WriteStatus> statuses = client.upsert(Collections.singletonList(tombstone), instant);
    assertNoErrors(statuses);
    commit(client, instant, statuses);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCommitTimeOrderingAcceptsLaterCommitWithOlderEventTime(boolean compactFirst) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig(RecordMergeMode.COMMIT_TIME_ORDERING, OverwriteWithLatestAvroPayload.class.getName());
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,9,9,par1", true);
      if (compactFirst) {
        compact(client);
      }
      writeAndCommit(client, "id1,later,1,1,par1", true, WriteOperationType.UPSERT);
      Map<String, String> expected = Collections.singletonMap("id1", "id1,later,1,1,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
    }
  }

  /** Empty batches must create valid commits without inventing data, for every public write entry point. */
  @ParameterizedTest
  @EnumSource(value = WriteOperationType.class, names = {"INSERT", "INSERT_PREPPED", "UPSERT", "UPSERT_PREPPED",
      "BULK_INSERT", "BULK_INSERT_PREPPED", "DELETE", "DELETE_PREPPED"})
  public void testEmptyWriteOperations(WriteOperationType operation) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,keep,1,1,par1", true);
      String instant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instant);
      List<HoodieRecord> empty = new ArrayList<>();
      List<WriteStatus> statuses;
      switch (operation) {
        case INSERT:
          statuses = client.insert(empty, instant);
          break;
        case INSERT_PREPPED:
          statuses = client.insertPreppedRecords(empty, instant);
          break;
        case UPSERT:
          statuses = client.upsert(empty, instant);
          break;
        case UPSERT_PREPPED:
          statuses = client.upsertPreppedRecords(empty, instant);
          break;
        case BULK_INSERT:
          statuses = client.bulkInsert(empty, instant);
          break;
        case BULK_INSERT_PREPPED:
          statuses = client.bulkInsertPreppedRecords(empty, instant, Option.empty());
          break;
        case DELETE:
          statuses = client.delete(new ArrayList<>(), instant);
          break;
        case DELETE_PREPPED:
          statuses = client.deletePrepped(empty, instant);
          break;
        default:
          throw new IllegalArgumentException("Unexpected operation " + operation);
      }
      assertNoErrors(statuses);
      assertTrue(statuses.isEmpty());
      commit(client, instant, statuses);
      assertEquals(Collections.singletonMap("id1", "id1,keep,1,1,par1"), readAll(reader.readSnapshot()));
      assertTrue(readAllRecordKeys(reader.readIncremental(completionTimeOf(instant), completionTimeOf(instant))).isEmpty());
    }
  }

  @Test
  public void testPartitionScopedKeysRemainDistinctThroughDeleteAndCompaction() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "same,left,1,1,par1", true);
      writeAndCommit(client, "same,right,2,1,par2", true);
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size(), "non-global index keys are scoped by partition");
      compact(client);
      assertEquals(2, readAllRecordKeys(reader.readOptimized()).size());
      String instant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instant);
      List<WriteStatus> statuses = client.delete(new ArrayList<>(Collections.singletonList(new HoodieKey("same", "par1"))), instant);
      assertNoErrors(statuses);
      commit(client, instant, statuses);
      Map<String, String> expected = Collections.singletonMap("same", "same,right,2,1,par2");
      assertEquals(expected, readAll(reader.readSnapshot()));
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(Collections.singletonList("same"), readAllRecordKeys(reader.readSnapshot()));
    }
  }

  /** Java exposes overwrite executors on the table API; verify replace commits at that actual boundary. */
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  public void testOverwriteReplaceCommitVisibility(boolean overwriteTable, boolean compactFirst) throws Exception {
    HoodieWriteConfig config = singleWriterInMemoryConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,replaced,1,1,par1", true);
      String beforeReplace = writeAndCommit(client, "id2,untouched,2,1,par2", true);
      Map<String, String> original = readAll(reader.readSnapshot());
      if (compactFirst) {
        compact(client);
      }
      String replace = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, replace, HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieJavaTable table = HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(metaClient));
      client.preWrite(replace, overwriteTable ? WriteOperationType.INSERT_OVERWRITE_TABLE : WriteOperationType.INSERT_OVERWRITE,
          table.getMetaClient());
      List<HoodieRecord> replacement = Collections.singletonList(str2HoodieRecord("id3,replacement,3,3,par1"));
      HoodieWriteMetadata<List<WriteStatus>> metadata = overwriteTable
          ? table.insertOverwriteTable(context, replace, replacement) : table.insertOverwrite(context, replace, replacement);
      assertNoErrors(metadata.getWriteStatuses());
      assertFalse(metadata.getPartitionToReplaceFileIds().isEmpty(), "replace metadata must identify the old file groups");
      assertTrue(client.commit(replace, metadata.getWriteStatuses(), Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION,
          metadata.getPartitionToReplaceFileIds()));
      Map<String, String> expected = new HashMap<>();
      expected.put("id3", "id3,replacement,3,3,par1");
      if (!overwriteTable) {
        expected.put("id2", "id2,untouched,2,1,par2");
      }
      Map<String, String> snapshot = readAll(reader.readSnapshot());
      List<String> keys = readAllRecordKeys(reader.readSnapshot());
      Map<String, String> historical = readAll(reader.readSnapshot(beforeReplace));
      String completion = completionTimeOf(replace);
      Map<String, String> incremental = readAll(reader.readIncremental(completion, completion));
      List<String> skipped = readAllRecordKeys(reader.readIncremental(completion, completion,
          HoodieJavaReadClient.IncrementalConfig.builder().skipInsertOverwrite(true).build()));
      assertTrue(client.rollback(replace));
      assertAll(
          () -> assertEquals(expected, snapshot),
          () -> assertEquals(expected.size(), keys.size()),
          () -> assertEquals(original, historical, "time travel must still see replaced file groups"),
          () -> assertEquals(Collections.singletonMap("id3", "id3,replacement,3,3,par1"), incremental),
          () -> assertTrue(skipped.isEmpty()),
          () -> assertEquals(original, readAll(reader.readSnapshot()), "rollback must make replaced file groups visible again"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testMetadataAndFilesystemListingAgreeAfterMorLifecycle(boolean enableMetadata) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadata).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    TypedProperties metadataReadProps = new TypedProperties();
    metadataReadProps.setProperty(HoodieMetadataConfig.ENABLE.key(), String.valueOf(enableMetadata));
    TypedProperties filesystemReadProps = new TypedProperties();
    filesystemReadProps.setProperty(HoodieMetadataConfig.ENABLE.key(), "false");
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient metadataReader = new HoodieJavaReadClient(context, metaClient, metadataReadProps);
         HoodieJavaReadClient filesystemReader = new HoodieJavaReadClient(context, metaClient, filesystemReadProps)) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      writeAndCommit(client, "id2,untouched,2,1,par2", true);
      writeAndCommit(client, "id1,updated,3,3,par1", true, WriteOperationType.UPSERT);
      compact(client);
      String rolledBack = writeAndCommit(client, "id3,rollback,4,4,par3", true);
      assertTrue(client.rollback(rolledBack));
      if (enableMetadata) {
        assertTrue(HoodieTableMetaClient.reload(metaClient).getTableConfig().isMetadataTableAvailable(),
            "the fixture must initialize metadata rather than silently exercising filesystem fallback");
      }
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,updated,3,3,par1");
      expected.put("id2", "id2,untouched,2,1,par2");
      assertEquals(expected, readAll(metadataReader.readSnapshot()));
      assertEquals(expected, readAll(metadataReader.readOptimized()));
      assertEquals(expected, readAll(filesystemReader.readSnapshot()));
      assertEquals(2, readAllRecordKeys(metadataReader.readSnapshot()).size());
    }
  }

  /** Spark/Flink precombine parity: input order and repeated keys must not change the event-time winner. */
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  public void testDuplicateInputUsesEventTimeWinner(boolean reverseInput, boolean compactFirst) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withProps(nbccBucketConfig(RecordMergeMode.EVENT_TIME_ORDERING, EventTimeAvroPayload.class.getName()).getProps())
        .combineInput(true, true).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeEventTimeAndCommit(client, "id1,original,1,1,par1", WriteOperationType.INSERT);
      if (compactFirst) {
        compact(client);
      }
      List<HoodieRecord> records = new ArrayList<>();
      for (String value : new String[] {"id1,winner,9,9,par1", "id1,stale,3,3,par1", "id1,older,2,2,par1",
          "id2,,4,4,par2"}) {
        GenericRecord record = str2GenericRecord(value);
        records.add(new HoodieAvroRecord<>(new HoodieKey((String) record.get("id"), (String) record.get("part")),
            new EventTimeAvroPayload(record, (Long) record.get("ts"))));
      }
      if (reverseInput) {
        Collections.reverse(records);
      }
      String instant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instant);
      List<WriteStatus> statuses = client.upsert(records, instant);
      assertNoErrors(statuses);
      commit(client, instant, statuses);
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,winner,9,9,par1");
      expected.put("id2", "id2,null,4,4,par2");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
    }
  }

  /** Partition deletion is a Spark/Flink operation that the inherited Java MOR table currently rejects. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testDeletePartitionsPreservesUnselectedPartition(boolean compactFirst) throws Exception {
    HoodieWriteConfig config = singleWriterInMemoryConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,remove,1,1,par1", true);
      writeAndCommit(client, "id2,keep,2,1,par2", true);
      if (compactFirst) {
        compact(client);
      }
      String instant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instant, HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieJavaTable table = HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(metaClient));
      client.preWrite(instant, WriteOperationType.DELETE_PARTITION, table.getMetaClient());
      HoodieWriteMetadata<List<WriteStatus>> metadata = table.deletePartitions(context, instant, Collections.singletonList("par1"));
      assertNoErrors(metadata.getWriteStatuses());
      assertTrue(client.commit(instant, metadata.getWriteStatuses(), Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION,
          metadata.getPartitionToReplaceFileIds()));
      assertEquals(Collections.singletonMap("id2", "id2,keep,2,1,par2"), readAll(reader.readSnapshot()));
      assertEquals(Collections.singletonList("id2"), readAllRecordKeys(reader.readSnapshot()));
    }
  }

  /** Acceptance contract for log-compaction rollback, even while execution itself is unimplemented. */
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  public void testLogCompactionRollbackPreservesSnapshot(boolean compactFirst, boolean completeLogCompaction) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1)
            .withLogCompactionEnabled(true).withLogCompactionBlocksThreshold(1).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      if (compactFirst) {
        compact(client);
      }
      writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
      Map<String, String> expected = Collections.singletonMap("id1", "id1,updated,2,2,par1");
      Option<String> instant = client.scheduleLogCompaction(Option.empty());
      assertTrue(instant.isPresent());
      HoodieWriteMetadata<List<WriteStatus>> metadata = client.logCompact(instant.get(), completeLogCompaction);
      assertNoErrors(metadata.getWriteStatuses());
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertTrue(client.rollback(instant.get()));
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertFalse(metaClient.reloadActiveTimeline().getCommitsAndCompactionTimeline().containsInstant(instant.get()));
      writeAndCommit(client, "id1,afterRollback,3,3,par1", true, WriteOperationType.UPSERT);
      compact(client);
      assertEquals(Collections.singletonMap("id1", "id1,afterRollback,3,3,par1"), readAll(reader.readOptimized()));
    }
  }

  /** Spark log-compaction archival acceptance contract: archived service instants must not lose live rows. */
  @Test
  public void testLogCompactionArchivalPreservesSnapshot() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1)
            .withLogCompactionEnabled(true).withLogCompactionBlocksThreshold(1).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).withAutoArchive(false).build())
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
      Option<String> logCompaction = client.scheduleLogCompaction(Option.empty());
      assertTrue(logCompaction.isPresent());
      HoodieWriteMetadata<List<WriteStatus>> metadata = client.logCompact(logCompaction.get(), true);
      assertNoErrors(metadata.getWriteStatuses());
      for (int i = 3; i <= 10; i++) {
        writeAndCommit(client, "id1,value" + i + "," + i + "," + i + ",par1", true, WriteOperationType.UPSERT);
        compact(client);
      }
      client.archive();
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertFalse(metaClient.getActiveTimeline().containsInstant(logCompaction.get()), "the fixture must really archive log compaction");
      assertTrue(metaClient.getArchivedTimeline().containsInstant(logCompaction.get()));
      Map<String, String> expected = Collections.singletonMap("id1", "id1,value10,10,10,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(Collections.singletonList("id1"), readAllRecordKeys(reader.readSnapshot()));
    }
  }

  /** Spark insert uses its own combine switch, independently of the upsert combine setting. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testInsertRespectsSeparateCombineSetting(boolean combineBeforeInsert) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withProps(nbccBucketConfig(RecordMergeMode.EVENT_TIME_ORDERING, EventTimeAvroPayload.class.getName()).getProps())
        .combineInput(combineBeforeInsert, !combineBeforeInsert).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      List<HoodieRecord> input = new ArrayList<>();
      for (String row : new String[] {"id1,winner,9,9,par1", "id1,older,2,2,par1"}) {
        GenericRecord record = str2GenericRecord(row);
        input.add(new HoodieAvroRecord<>(new HoodieKey("id1", "par1"), new EventTimeAvroPayload(record, (Long) record.get("ts"))));
      }
      String instant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instant);
      List<WriteStatus> statuses = client.insert(input, instant);
      assertNoErrors(statuses);
      long written = statuses.stream().mapToLong(status -> status.getStat().getNumWrites()).sum();
      commit(client, instant, statuses);
      assertAll(
          () -> assertEquals(combineBeforeInsert ? 1L : 2L, written, "insert must honor its own deduplication configuration"),
          () -> assertEquals(Collections.singletonMap("id1", "id1,winner,9,9,par1"), readAll(reader.readSnapshot())));
    }
  }

  /** Fixed-seed state-machine oracle checks mixed operation sequences after every transition. */
  @ParameterizedTest
  @ValueSource(longs = {7L, 29L, 101L})
  public void testMixedMorOperationsAgainstReferenceModel(long seed) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withAutoClean(false).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().withAutoArchive(false).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    Random random = new Random(seed);
    Map<String, String> expected = new TreeMap<>();
    Map<String, String> optimized = new TreeMap<>();
    List<Executable> checks = new ArrayList<>();
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      for (int step = 0; step < 48; step++) {
        int id = random.nextInt(12);
        String key = "id" + id;
        String value = key + ",seed" + seed + "step" + step + "," + step + "," + (step + 1) + ",par" + (id % 4);
        switch (step % 6) {
          case 0:
          case 1:
            String instant = writeAndCommit(client, value, true, expected.containsKey(key) ? WriteOperationType.UPSERT : WriteOperationType.INSERT);
            expected.put(key, value);
            recordModelCheck(checks, "incremental write at step " + step, Collections.singletonMap(key, value),
                readAll(reader.readIncremental(completionTimeOf(instant), completionTimeOf(instant))));
            break;
          case 2:
            if (!expected.isEmpty()) {
              String deletedKey = new ArrayList<>(expected.keySet()).get(random.nextInt(expected.size()));
              String[] fields = expected.remove(deletedKey).split(",");
              String deletion = WriteClientTestUtils.createNewInstantTime();
              WriteClientTestUtils.startCommitWithTime(client, deletion);
              List<WriteStatus> statuses = client.delete(new ArrayList<>(Collections.singletonList(new HoodieKey(deletedKey, fields[4]))), deletion);
              assertNoErrors(statuses);
              commit(client, deletion, statuses);
              recordModelCheck(checks, "incremental delete at step " + step, Collections.emptyMap(),
                  readAll(reader.readIncremental(completionTimeOf(deletion), completionTimeOf(deletion))));
            }
            break;
          case 3:
            compact(client);
            optimized = new TreeMap<>(expected);
            break;
          case 4:
            String rolledBack = writeAndCommit(client, value, true, WriteOperationType.UPSERT);
            Map<String, String> intermediate = new TreeMap<>(expected);
            intermediate.put(key, value);
            recordModelCheck(checks, "before rollback at step " + step, intermediate, readAll(reader.readSnapshot()));
            assertTrue(client.rollback(rolledBack));
            break;
          case 5:
            recordModelCheck(checks, "earliest incremental after rollback at step " + step, expected,
                readAll(reader.readIncremental(IncrementalQueryAnalyzer.START_COMMIT_EARLIEST, null)));
            break;
          default:
            throw new IllegalStateException("Unexpected state-machine action");
        }
        String context = "seed=" + seed + ", step=" + step;
        recordModelCheck(checks, "snapshot " + context, expected, readAll(reader.readSnapshot()));
        int expectedCount = expected.size();
        int actualCount = readAllRecordKeys(reader.readSnapshot()).size();
        checks.add(() -> assertEquals(expectedCount, actualCount, "row multiplicity " + context));
        recordModelCheck(checks, "read-optimized " + context, optimized, readAll(reader.readOptimized()));
      }
      assertAll("MOR reference model, seed=" + seed, checks);
    }
  }

  private void recordModelCheck(List<Executable> checks, String context, Map<String, String> expected, Map<String, String> actual) {
    Map<String, String> expectedSnapshot = new TreeMap<>(expected);
    checks.add(() -> assertEquals(expectedSnapshot, actual, context));
  }

  @ParameterizedTest
  @EnumSource(HoodieCleaningPolicy.class)
  public void testCleanerPoliciesRetainCurrentMorSnapshot(HoodieCleaningPolicy policy) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withAutoClean(false).withCleanerPolicy(policy)
            .retainCommits(1).retainFileVersions(1).cleanerNumHoursRetained(1).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().withAutoArchive(false).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      for (int i = 1; i <= 4; i++) {
        writeAndCommit(client, "id1,value" + i + "," + i + "," + i + ",par1", true, WriteOperationType.UPSERT);
        compact(client);
      }
      org.apache.hudi.avro.model.HoodieCleanMetadata clean = client.clean();
      if (policy == HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS) {
        assertTrue(clean == null || clean.getTotalFilesDeleted() == 0, "hour retention must preserve these recent file slices");
      } else {
        assertNotNull(clean);
        assertTrue(clean.getTotalFilesDeleted() > 0, "commit/version retention must actually delete obsolete file slices");
      }
      Map<String, String> expected = Collections.singletonMap("id1", "id1,value4,4,4,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(expected, readAll(reader.readOptimized()));
      writeAndCommit(client, "id1,afterClean,5,5,par1", true, WriteOperationType.UPSERT);
      compact(client);
      assertEquals(Collections.singletonMap("id1", "id1,afterClean,5,5,par1"), readAll(reader.readOptimized()));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testClusteringPreservesSnapshotAndIncrementalBoundaries(boolean appendLogs) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(singleWriterInMemoryConfig().getProps())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withClusteringSortColumns("id")
            .withClusteringTargetPartitions(0).withClusteringMaxNumGroups(10)
            .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName()).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      String originalInstant = writeAndCommit(client, "id1,original,1,1,par1", true);
      compact(client);
      if (appendLogs) {
        writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
      }
      Map<String, String> expected = readAll(reader.readSnapshot());
      Option<String> clustering = client.scheduleClustering(Option.empty());
      assertTrue(clustering.isPresent());
      client.cluster(clustering.get(), true);
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(1, readAllRecordKeys(reader.readSnapshot()).size());
      assertEquals(Collections.singletonMap("id1", "id1,original,1,1,par1"), readAll(reader.readSnapshot(originalInstant)));
      String completion = completionTimeOf(clustering.get());
      assertTrue(readAllRecordKeys(reader.readIncremental(completion, completion)).isEmpty(), "default incremental reads skip clustering");
      writeAndCommit(client, "id1,afterCluster,3,3,par1", true, WriteOperationType.UPSERT);
      assertEquals(Collections.singletonMap("id1", "id1,afterCluster,3,3,par1"), readAll(reader.readIncremental(completion, null)));
      compact(client);
      assertEquals(Collections.singletonMap("id1", "id1,afterCluster,3,3,par1"), readAll(reader.readOptimized()));
    }
  }

  @ParameterizedTest
  @EnumSource(PartitionTTLStrategyType.class)
  public void testPartitionTtlRemovesExpiredPartitions(PartitionTTLStrategyType strategy) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(singleWriterInMemoryConfig().getProps())
        .withTTLConfig(HoodieTTLConfig.newBuilder().withTTLDaysRetain(1).withTTLStrategyType(strategy).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeData(client, "20200101000000000", "id1,expired,1,1,par1", true);
      writeAndCommit(client, "id2,current,2,2,par2", true);
      String instant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instant, HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieJavaTable table = HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(metaClient));
      client.preWrite(instant, WriteOperationType.DELETE_PARTITION, table.getMetaClient());
      HoodieWriteMetadata<List<WriteStatus>> metadata = table.managePartitionTTL(context, instant);
      assertNoErrors(metadata.getWriteStatuses());
      assertEquals(Collections.singleton("par1"), metadata.getPartitionToReplaceFileIds().keySet());
      assertTrue(metadata.isCommitted(), "TTL must commit its replace action when called by inline table services");
      assertEquals(Collections.singletonMap("id2", "id2,current,2,2,par2"), readAll(reader.readSnapshot()));
    }
  }

  /** Bootstrap acceptance uses an actual external Parquet source, not a missing-path placeholder. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testBootstrapExternalParquetIntoMor(boolean fullBootstrap) throws Exception {
    HoodieWriteConfig sourceConfig = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, sourceConfig.getProps());
    Path sourceRoot = tempDir.resolve("external-bootstrap-source");
    Path sourcePartition = sourceRoot.resolve("par1");
    Files.createDirectories(sourcePartition);
    try (HoodieJavaWriteClient source = getHoodieWriteClient(sourceConfig, false)) {
      writeAndCommit(source, "id1,external,1,1,par1", true);
      compact(source);
      try (SyncableFileSystemView view = getFileSystemView(metaClient.reloadActiveTimeline())) {
        String path = view.getLatestBaseFiles("par1").findFirst().get().getPath();
        Files.copy(Path.of(new org.apache.hudi.storage.StoragePath(path).toUri().getPath()), sourcePartition.resolve("input.parquet"));
      }
    }
    String target = tempDir.resolve("bootstrap-target").toString();
    Properties bootstrapKeys = new Properties();
    bootstrapKeys.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    bootstrapKeys.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "part");
    bootstrapKeys.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id");
    bootstrapKeys.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "part");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(singleWriterInMemoryConfig().getProps())
        .withProps(bootstrapKeys).withPath(target)
        .withBootstrapConfig(HoodieBootstrapConfig.newBuilder().withBootstrapBasePath(sourceRoot.toString())
            .withBootstrapModeSelector(fullBootstrap ? FullRecordBootstrapModeSelector.class.getName()
                : MetadataOnlyBootstrapModeSelector.class.getName()).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, target, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaTable table = HoodieJavaTable.create(config, context, metaClient);
    HoodieBootstrapWriteMetadata<List<WriteStatus>> result = table.bootstrap(context, Option.empty());
    Option<HoodieWriteMetadata<List<WriteStatus>>> metadata = fullBootstrap ? result.getFullBootstrapResult() : result.getMetadataBootstrapResult();
    assertTrue(metadata.isPresent());
    assertNoErrors(metadata.get().getWriteStatuses());
    try (HoodieJavaReadClient reader = newReadClient()) {
      assertEquals(Collections.singletonMap("id1", "id1,external,1,1,par1"), readAll(reader.readSnapshot()));
      assertEquals(Collections.singletonList("id1"), readAllRecordKeys(reader.readSnapshot()));
    }
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
      compact(client);
      Map<String, String> updated = Collections.singletonMap("id1", "id1,updated,2,2,par1");
      assertEquals(updated, readAll(reader.readSnapshot()));
      assertEquals(updated, readAll(reader.readOptimized()));
      assertEquals(1, readAllRecordKeys(reader.readSnapshot()).size());
    }
    table = HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(metaClient));
    table.rollbackBootstrap(context, WriteClientTestUtils.createNewInstantTime());
    assertTrue(Files.exists(sourcePartition.resolve("input.parquet")), "bootstrap rollback must preserve the external source");
    try (HoodieJavaReadClient reader = newReadClient()) {
      assertTrue(readAll(reader.readSnapshot()).isEmpty());
    }
  }

  /** Real multi-file-group scale regression: one spill buffer at a time while planning dozens of slices. */
  @ParameterizedTest
  @ValueSource(strings = {"BITCASK", "ROCKS_DB"})
  public void testManyFileGroupsKeepOneSpillBufferOpen(String diskMapType) throws Exception {
    HoodieWriteConfig baseConfig = nbccBucketConfig();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(baseConfig.getProps())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(baseConfig.getProps())
            .withIndexType(HoodieIndex.IndexType.BUCKET).withBucketNum("4").build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    Set<String> fileGroups = new HashSet<>();
    Map<String, String> expected = new TreeMap<>();
    Path spillDirectory = tempDir.resolve("multi-file-group-spill");
    Files.createDirectories(spillDirectory);
    TypedProperties readProps = new TypedProperties();
    readProps.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), "1");
    readProps.setProperty(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), diskMapType);
    readProps.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), spillDirectory.toString());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false)) {
      for (int batch = 0; batch < 2; batch++) {
        List<HoodieRecord> records = new ArrayList<>();
        for (int i = 0; i < 512; i++) {
          String value = "id" + i + ",batch" + batch + "," + i + "," + batch + ",par" + (i % 16);
          records.add(str2HoodieRecord(value));
          expected.put("id" + i, value);
        }
        String instant = WriteClientTestUtils.createNewInstantTime();
        WriteClientTestUtils.startCommitWithTime(client, instant);
        List<WriteStatus> statuses = client.upsert(records, instant);
        assertNoErrors(statuses);
        statuses.forEach(status -> fileGroups.add(status.getStat().getPartitionPath() + "/" + status.getStat().getFileId()));
        commit(client, instant, statuses);
      }
      assertTrue(fileGroups.size() >= 32, "the fixture must exercise many file groups, not one hot bucket");
      Map<String, String> actual = new TreeMap<>();
      int count = 0;
      try (HoodieJavaReadClient reader = new HoodieJavaReadClient(context, metaClient, readProps);
           ClosableIterator<HoodieRecord<IndexedRecord>> iterator = reader.readSnapshot()) {
        try (Stream<Path> files = Files.list(spillDirectory)) {
          assertEquals(0, files.count(), "planning must not open spill buffers before iteration");
        }
        while (iterator.hasNext()) {
          try (Stream<Path> files = Files.list(spillDirectory)) {
            assertEquals(1, files.count(), "only the current file group's spill buffer may remain open");
          }
          HoodieRecord<IndexedRecord> record = iterator.next();
          actual.put(record.getRecordKey(), summarize((GenericRecord) record.getData()));
          count++;
        }
      }
      assertEquals(512, count);
      assertEquals(expected, actual);
      try (Stream<Path> files = Files.list(spillDirectory)) {
        assertEquals(0, files.count(), "exhaustion must remove the final spill buffer");
      }
    }
  }

  /** Hadoop/Flink schema-evolution contract: old base/log records receive a new field's Avro default. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testAddedSchemaFieldDefaultAcrossOldBaseAndLogs(boolean compactFirst) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false)) {
      writeAndCommit(client, "id1,old,1,1,par1", true);
      if (compactFirst) {
        compact(client);
      }
    }
    String evolvedJson = jsonSchema.replace("  ]", ", {\"name\":\"country\",\"type\":\"string\",\"default\":\"unknown\"}  ]");
    HoodieSchema evolvedSchema = HoodieSchema.parse(evolvedJson);
    HoodieWriteConfig evolvedConfig = HoodieWriteConfig.newBuilder().withProps(config.getProps()).withSchema(evolvedJson).build();
    try (HoodieJavaWriteClient client = getHoodieWriteClient(evolvedConfig, false);
         HoodieJavaReadClient reader = newReadClient()) {
      GenericRecord record = new GenericData.Record(evolvedSchema.toAvroSchema());
      record.put("id", "id2");
      record.put("name", "new");
      record.put("age", 2);
      record.put("ts", 2L);
      record.put("part", "par1");
      record.put("country", "Canada");
      HoodieRecord incoming = new HoodieAvroRecord<>(new HoodieKey("id2", "par1"), new OverwriteWithLatestAvroPayload(record, 2L));
      String instant = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, instant);
      List<WriteStatus> statuses = client.insert(Collections.singletonList(incoming), instant);
      assertNoErrors(statuses);
      commit(client, instant, statuses);
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "unknown");
      expected.put("id2", "Canada");
      for (int pass = 0; pass < 2; pass++) {
        if (pass == 1) {
          compact(client);
        }
        Map<String, String> countries = new HashMap<>();
        try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator = reader.readSnapshot()) {
          while (iterator.hasNext()) {
            HoodieRecord<IndexedRecord> row = iterator.next();
            countries.put(row.getRecordKey(), String.valueOf(((GenericRecord) row.getData()).get("country")));
          }
        }
        assertEquals(expected, countries, "new-field defaults must survive mixed writer schemas and compaction");
        assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
      }
    }
  }

  /** Meta-field-free MOR snapshot/optimized reads remain a contract even though incremental is excluded. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSnapshotWithoutMetadataFields(boolean compactFirst) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(singleWriterInMemoryConfig().getProps())
        .withPopulateMetaFields(false).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      if (compactFirst) {
        compact(client);
      }
      writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
      Map<String, String> expected = Collections.singletonMap("id1", "id1,updated,2,2,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(Collections.singletonList("id1"), readAllRecordKeys(reader.readSnapshot()));
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
    }
  }

  /** Preserve complex Avro values and their schema through Java MOR writes and table services. */
  @ParameterizedTest
  @CsvSource({"8,false", "8,true", "9,false", "9,true", "10,false", "10,true"})
  public void testComplexValuesSurviveMorCompactionAndClustering(int version, boolean compactFirst) throws Exception {
    // Spark's fixed-decimal regression must also hold for the standalone Avro writer. Include
    // nested, binary, collection and logical values so scalar-only fixtures cannot hide data loss.
    schema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"complexRecord\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"part\",\"type\":\"string\"},"
        + "{\"name\":\"ts\",\"type\":\"long\"},"
        + "{\"name\":\"amount\",\"type\":{\"type\":\"fixed\",\"name\":\"amountFixed\",\"size\":10,"
        + "\"logicalType\":\"decimal\",\"precision\":20,\"scale\":2}},"
        + "{\"name\":\"details\",\"type\":{\"type\":\"record\",\"name\":\"detailsRecord\",\"fields\":["
        + "{\"name\":\"label\",\"type\":[\"null\",\"string\"],\"default\":null}]}},"
        + "{\"name\":\"numbers\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},"
        + "{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
        + "{\"name\":\"bytes\",\"type\":\"bytes\"},"
        + "{\"name\":\"day\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
        + "{\"name\":\"eventTime\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}}]}");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withEngineType(EngineType.JAVA)
        .withProps(singleWriterInMemoryConfig().getProps()).withSchema(schema.toString()).withWriteTableVersion(version)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withClusteringSortColumns("id")
            .withClusteringTargetPartitions(0).withClusteringMaxNumGroups(10)
            .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName()).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      GenericRecord first = complexRecord(1);
      writeComplexRecord(client, first);
      assertComplexRecord(first, reader.readSnapshot());
      if (compactFirst) {
        compact(client);
        assertComplexRecord(first, reader.readOptimized());
      } else {
        assertTrue(readAllRecordKeys(reader.readOptimized()).isEmpty(), "the first fixture must really be log-only");
      }
      GenericRecord updated = complexRecord(2);
      String update = writeComplexRecord(client, updated);
      assertComplexRecord(updated, reader.readSnapshot());
      assertComplexRecord(updated, reader.readIncremental(completionTimeOf(update), completionTimeOf(update)));
      compact(client);
      assertComplexRecord(updated, reader.readSnapshot());
      assertComplexRecord(updated, reader.readOptimized());
      Option<String> clustering = client.scheduleClustering(Option.empty());
      assertTrue(clustering.isPresent(), "clustering must actually rewrite the compacted file");
      HoodieWriteMetadata<List<WriteStatus>> clustered = client.cluster(clustering.get(), true);
      assertNoErrors(clustered.getWriteStatuses());
      assertComplexRecord(updated, reader.readSnapshot());
      assertComplexRecord(updated, reader.readOptimized());
      GenericRecord afterClustering = complexRecord(3);
      writeComplexRecord(client, afterClustering);
      assertComplexRecord(afterClustering, reader.readSnapshot());
      compact(client);
      assertComplexRecord(afterClustering, reader.readOptimized());
    }
  }

  private GenericRecord complexRecord(long generation) {
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "id1");
    record.put("part", "par1");
    record.put("ts", generation);
    byte[] amount = new byte[10];
    if (generation == 2) {
      Arrays.fill(amount, (byte) 0xff);
    }
    amount[9] = (byte) generation;
    record.put("amount", new GenericData.Fixed(schema.toAvroSchema().getField("amount").schema(), amount));
    GenericRecord details = new GenericData.Record(schema.toAvroSchema().getField("details").schema());
    details.put("label", generation == 2 ? null : "héllo-世界-" + generation);
    record.put("details", details);
    record.put("numbers", generation == 2 ? Collections.emptyList() : Arrays.asList(Long.MIN_VALUE, generation, Long.MAX_VALUE));
    record.put("attributes", generation == 2 ? Collections.emptyMap() : Collections.singletonMap("key", "世界-" + generation));
    record.put("bytes", ByteBuffer.wrap(generation == 2 ? new byte[0] : new byte[] {0, (byte) generation, (byte) 0xff}));
    record.put("day", (int) generation - 2);
    record.put("eventTime", generation == 1 ? -1L : 253402300799999999L);
    return record;
  }

  private String writeComplexRecord(HoodieJavaWriteClient client, GenericRecord record) {
    String instant = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> statuses = client.upsert(Collections.singletonList(new HoodieAvroRecord<>(
        new HoodieKey("id1", "par1"), new OverwriteWithLatestAvroPayload(record, (Long) record.get("ts")))), instant);
    assertNoErrors(statuses);
    commit(client, instant, statuses);
    return instant;
  }

  private void assertComplexRecord(GenericRecord expected, ClosableIterator<HoodieRecord<IndexedRecord>> rows) {
    try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator = rows) {
      assertTrue(iterator.hasNext());
      GenericRecord actual = (GenericRecord) iterator.next().getData();
      GenericRecord projected = new GenericData.Record(expected.getSchema());
      expected.getSchema().getFields().forEach(field -> projected.put(field.name(), actual.get(field.name())));
      // Avro log readers may expose physical primitives while Parquet/native readers apply Avro's
      // logical conversions. Compare the exact day/microsecond values across both representations.
      if (projected.get("day") instanceof LocalDate) {
        projected.put("day", Math.toIntExact(((LocalDate) projected.get("day")).toEpochDay()));
      }
      if (projected.get("eventTime") instanceof Instant) {
        Instant timestamp = (Instant) projected.get("eventTime");
        projected.put("eventTime", timestamp.getEpochSecond() * 1000000L + timestamp.getNano() / 1000);
      }
      assertEquals(expected, projected, "all values, including empty and null values, must survive the rewrite");
      assertEquals(expected.getSchema().getField("amount").schema(),
          actual.getSchema().getField("amount").schema(), "decimal precision, scale and fixed width must be preserved");
      assertFalse(iterator.hasNext(), "the update must not leave duplicate rows");
    }
  }

  /** Every advertised base/log format at each completion-time timeline version, without pairwise omissions. */
  private static Stream<Arguments> storageCompatibilityCases() {
    return Stream.concat(Stream.of(8, 9).flatMap(version -> Stream.of("PARQUET", "ORC", "HFILE")
        .flatMap(base -> Stream.of("avro", "parquet", "hfile")
            .map(log -> Arguments.of(version, base, log)))),
        Stream.of("PARQUET", "ORC", "HFILE").map(base -> Arguments.of(10, base, "native")));
  }

  @ParameterizedTest(name = "version={0}, base={1}, log={2}")
  @MethodSource("storageCompatibilityCases")
  public void testStorageCompatibilityLifecycle(int version, String baseFormat, String logFormat) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withWriteTableVersion(version).withBaseFileFormat(baseFormat)
        .withProps(Collections.singletonMap(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFormat))
        .withStorageConfig(HoodieStorageConfig.newBuilder().logFileDataBlockFormat("native".equals(logFormat) ? "avro" : logFormat).build()).build();
    assertStorageLifecycle(config);
  }

  @ParameterizedTest
  @CsvSource({"8,UNCOMPRESSED", "8,SNAPPY", "8,GZIP", "8,ZSTD", "9,UNCOMPRESSED", "9,SNAPPY", "9,GZIP", "9,ZSTD",
      "10,UNCOMPRESSED", "10,SNAPPY", "10,GZIP", "10,ZSTD"})
  public void testParquetCodecLifecycle(int version, String codec) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withWriteTableVersion(version)
        .withStorageConfig(HoodieStorageConfig.newBuilder().logFileDataBlockFormat("parquet").parquetCompressionCodec(codec).build()).build();
    assertStorageLifecycle(config);
    int checkedFiles = 0;
    try (Stream<Path> paths = Files.walk(java.nio.file.Paths.get(basePath))) {
      for (Path path : paths.filter(path -> path.toString().endsWith(".parquet") && !path.toString().contains("/.hoodie/"))
          .collect(Collectors.toList())) {
        ParquetMetadata footer = ParquetFileReader.readFooter(storageConf.unwrapAs(org.apache.hadoop.conf.Configuration.class),
            new org.apache.hadoop.fs.Path(path.toUri()));
        for (org.apache.parquet.hadoop.metadata.BlockMetaData block : footer.getBlocks()) {
          for (ColumnChunkMetaData column : block.getColumns()) {
            assertEquals(codec, column.getCodec().name(), "configured codec must appear in the actual file footer: " + path);
          }
        }
        checkedFiles++;
      }
    }
    assertTrue(checkedFiles > 0, "codec acceptance must inspect real files");
  }

  private void assertStorageLifecycle(HoodieWriteConfig config) throws Exception {
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      String first = writeAndCommit(client, "id1,original,1,1,par1", true);
      try (SyncableFileSystemView view = getFileSystemView(metaClient.reloadActiveTimeline())) {
        List<HoodieLogFile> logs = view.getLatestFileSlices("par1").flatMap(slice -> slice.getLogFiles()).collect(Collectors.toList());
        assertFalse(logs.isEmpty(), "compatibility must actually exercise MOR logs");
        assertTrue(logs.stream().allMatch(log -> log.isNativeLogFile() == (config.getWriteVersion().versionCode() >= 10)),
            "v10 uses native logs; v8/v9 use inline data blocks");
      }
      writeAndCommit(client, "id2,keep,2,2,par2", true);
      writeAndCommit(client, "id1,updated,3,3,par1", true, WriteOperationType.UPSERT);
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,updated,3,3,par1");
      expected.put("id2", "id2,keep,2,2,par2");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(Collections.singletonMap("id1", "id1,original,1,1,par1"), readAll(reader.readSnapshot(first)));
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      String deleted = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, deleted);
      List<WriteStatus> statuses = client.delete(new ArrayList<>(Collections.singletonList(new HoodieKey("id1", "par1"))), deleted);
      assertNoErrors(statuses);
      commit(client, deleted, statuses);
      assertEquals(Collections.singletonMap("id2", "id2,keep,2,2,par2"), readAll(reader.readSnapshot()));
      assertTrue(client.rollback(deleted));
      assertEquals(expected, readAll(reader.readSnapshot()), "rollback restores a tombstone's previous value");
      String last = writeAndCommit(client, "id1,reinserted,4,4,par1", true, WriteOperationType.UPSERT);
      assertEquals(Collections.singletonMap("id1", "id1,reinserted,4,4,par1"),
          readAll(reader.readIncremental(completionTimeOf(last), completionTimeOf(last))));
      expected.put("id1", "id1,reinserted,4,4,par1");
      compact(client);
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(config.getWriteVersion(), metaClient.getTableConfig().getTableVersion(),
          "a compatibility test must not silently upgrade its table");
    }
  }

  private enum CommitFailurePhase {
    BEFORE_METADATA, AFTER_METADATA, AFTER_COMMIT
  }

  private static Stream<Arguments> commitFailureCases() {
    return Arrays.stream(CommitFailurePhase.values()).flatMap(phase -> Stream.of(false, true)
        .flatMap(base -> Stream.of(false, true).map(markers -> Arguments.of(phase, base, markers))));
  }

  /** Spark's pre/post-MDT crash boundaries, with an actual metadata write before AFTER_METADATA fails. */
  @ParameterizedTest
  @MethodSource("commitFailureCases")
  public void testCommitFailureRecovery(CommitFailurePhase phase, boolean compactFirst, boolean markers) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .withRollbackUsingMarkers(markers).withCanIgnorePostCommitFailures(false).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    String failed;
    try (FailingCommitClient client = new FailingCommitClient(config)) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      if (compactFirst) {
        compact(client);
      }
      failed = WriteClientTestUtils.createNewInstantTime();
      List<WriteStatus> statuses = writeData(client, failed, "id1,unacknowledged,2,2,par1", false, WriteOperationType.UPSERT);
      client.failurePhase = phase;
      RuntimeException error = assertThrows(RuntimeException.class, () -> commit(client, failed, statuses));
      assertTrue(error.toString().contains("injected"), "the requested boundary must actually be reached");
      assertEquals(1, client.injectedFailures);
    }
    Map<String, String> expected = Collections.singletonMap("id1", phase == CommitFailurePhase.AFTER_COMMIT
        ? "id1,unacknowledged,2,2,par1" : "id1,original,1,1,par1");
    try (HoodieJavaReadClient reader = newReadClient();
         HoodieJavaWriteClient recovery = getHoodieWriteClient(config, false)) {
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertEquals(phase == CommitFailurePhase.AFTER_COMMIT,
          metaClient.getActiveTimeline().filterCompletedInstants().containsInstant(failed));
      assertEquals(expected, readAll(reader.readSnapshot()), "only a completed data commit becomes visible");
      if (phase != CommitFailurePhase.AFTER_COMMIT) {
        assertTrue(recovery.rollback(failed));
      }
      String retry = writeAndCommit(recovery, "id1,recovered,3,3,par1", true, WriteOperationType.UPSERT);
      expected = Collections.singletonMap("id1", "id1,recovered,3,3,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      TypedProperties filesystem = new TypedProperties();
      filesystem.setProperty(HoodieMetadataConfig.ENABLE.key(), "false");
      try (HoodieJavaReadClient filesystemReader = new HoodieJavaReadClient(context, basePath, filesystem)) {
        assertEquals(readAll(filesystemReader.readSnapshot()), readAll(reader.readSnapshot()),
            "metadata listing must recover together with data listing");
      }
      assertEquals(expected, readAll(reader.readIncremental(completionTimeOf(retry), completionTimeOf(retry))));
      assertEquals(1, readAllRecordKeys(reader.readSnapshot()).size());
      compact(recovery);
      assertEquals(expected, readAll(reader.readOptimized()));
    }
  }

  private class FailingCommitClient extends HoodieJavaWriteClient {
    private CommitFailurePhase failurePhase;
    private int injectedFailures;

    FailingCommitClient(HoodieWriteConfig config) {
      super(TestHoodieJavaReadClient.this.context, config);
    }

    private void failAt(CommitFailurePhase phase) {
      if (phase == failurePhase) {
        failurePhase = null;
        injectedFailures++;
        throw new IllegalStateException("injected " + phase);
      }
    }

    @Override
    protected void writeToMetadataTable(boolean skip, HoodieTable table, String instant,
                                         List partialStats, HoodieCommitMetadata metadata) {
      failAt(CommitFailurePhase.BEFORE_METADATA);
      super.writeToMetadataTable(skip, table, instant, partialStats, metadata);
      failAt(CommitFailurePhase.AFTER_METADATA);
    }

    @Override
    protected void postCommit(HoodieTable table, HoodieCommitMetadata metadata, String instant,
                              String action, Option extraMetadata) {
      super.postCommit(table, metadata, instant, action, extraMetadata);
      failAt(CommitFailurePhase.AFTER_COMMIT);
    }
  }

  /** Measured bounded workload: skew, wide values, long log history, scan and compaction. */
  @Tag("performance")
  @ParameterizedTest
  @CsvSource({"256,16,false", "256,4096,true", "2048,16,true", "2048,4096,false"})
  public void testMeasuredMorWorkload(int rows, int width, boolean skewed) throws Exception {
    int scale = Integer.getInteger("java.mor.performance.scale", 1);
    assertTrue(scale > 0, "performance scale must be positive");
    rows = Math.multiplyExact(rows, scale);
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    long started = System.nanoTime();
    long heapBefore = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    com.sun.management.ThreadMXBean threadBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    assertTrue(threadBean.isThreadAllocatedMemorySupported(), "the measured workload requires HotSpot allocation counters");
    threadBean.setThreadAllocatedMemoryEnabled(true);
    long allocatedBefore = threadBean.getThreadAllocatedBytes(Thread.currentThread().getId());
    String payload = String.join("", Collections.nCopies(width, "x"));
    Map<String, String> expected = new HashMap<>();
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      for (int batch = 0; batch < 8; batch++) {
        String instant = WriteClientTestUtils.createNewInstantTime();
        WriteClientTestUtils.startCommitWithTime(client, instant);
        List<HoodieRecord> records = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
          String partition = "par" + (skewed && i % 10 != 0 ? 0 : i % 16);
          String row = "id" + i + "," + payload + "," + batch + "," + (batch + 1) + "," + partition;
          records.add(str2HoodieRecord(row));
          expected.put("id" + i, row);
        }
        List<WriteStatus> statuses = client.upsert(records, instant);
        assertNoErrors(statuses);
        commit(client, instant, statuses);
      }
      long writeNanos = System.nanoTime() - started;
      // Warm the reader before recording scan throughput; never trade correctness for speed.
      assertEquals(expected, readAll(reader.readSnapshot()));
      long scanStarted = System.nanoTime();
      for (int repeat = 0; repeat < 3; repeat++) {
        assertEquals(expected, readAll(reader.readSnapshot()));
        assertEquals(rows, readAllRecordKeys(reader.readSnapshot()).size());
      }
      long scanNanos = System.nanoTime() - scanStarted;
      long compactionStarted = System.nanoTime();
      compact(client);
      long compactionNanos = System.nanoTime() - compactionStarted;
      assertEquals(expected, readAll(reader.readOptimized()));
      long heapDelta = Math.max(0, ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() - heapBefore);
      long allocatedBytes = threadBean.getThreadAllocatedBytes(Thread.currentThread().getId()) - allocatedBefore;
      String measurement = String.format(java.util.Locale.ROOT,
          "rows=%d,width=%d,skewed=%s,write_ms=%d,scan_ms=%d,compaction_ms=%d,scan_rows_per_second=%.2f,heap_delta_bytes=%d,main_thread_allocated_bytes=%d%n",
          rows, width, skewed, TimeUnit.NANOSECONDS.toMillis(writeNanos), TimeUnit.NANOSECONDS.toMillis(scanNanos),
          TimeUnit.NANOSECONDS.toMillis(compactionNanos), rows * 6.0 * TimeUnit.SECONDS.toNanos(1) / scanNanos, heapDelta, allocatedBytes);
      Path report = java.nio.file.Paths.get("target", "mor-performance", rows + "-" + width + "-" + skewed + ".txt");
      Files.createDirectories(report.getParent());
      Files.write(report, measurement.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      // Generous configurable CI guard, not a claim of cross-engine performance equivalence.
      long budgetSeconds = Long.getLong("java.mor.performance.maxSeconds", 180L);
      assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(budgetSeconds), measurement);
      assertTrue(allocatedBytes < Long.getLong("java.mor.performance.maxAllocatedBytes", 8L * 1024 * 1024 * 1024), measurement);
    }
  }

  private enum StorageFailure {
    CREATE, WRITE, CLOSE, READ, DELETE
  }

  /** One-shot faults at the Hadoop boundary also intercept Parquet's direct filesystem access. */
  public static class FaultInjectingFileSystem extends LocalFileSystem {
    private static volatile StorageFailure failure;
    private static volatile String targetPartition;
    private static volatile boolean persistent;
    private static final AtomicInteger INJECTIONS = new AtomicInteger();

    private static synchronized void fail(StorageFailure point, org.apache.hadoop.fs.Path path) throws IOException {
      if (failure == point && path.toString().contains("/" + targetPartition + "/")
          && ("metadata".equals(targetPartition) || !path.toString().contains("/.hoodie/"))
          && !path.getName().startsWith(".hoodie_partition_metadata")) {
        if (!persistent) {
          failure = null;
        }
        INJECTIONS.incrementAndGet();
        throw new IOException("injected " + point + (point == StorageFailure.WRITE ? " No space left on device" : " transient I/O failure"));
      }
    }

    @Override
    public FSDataOutputStream create(org.apache.hadoop.fs.Path path, FsPermission permission, boolean overwrite,
                                      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
      fail(StorageFailure.CREATE, path);
      OutputStream stream = super.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
      return new FSDataOutputStream(new FilterOutputStream(stream) {
        @Override
        public void write(int value) throws IOException {
          fail(StorageFailure.WRITE, path);
          out.write(value);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
          fail(StorageFailure.WRITE, path);
          out.write(bytes, offset, length);
        }

        @Override
        public void close() throws IOException {
          super.close();
          fail(StorageFailure.CLOSE, path);
        }
      }, statistics);
    }

    @Override
    public FSDataInputStream open(org.apache.hadoop.fs.Path path, int bufferSize) throws IOException {
      fail(StorageFailure.READ, path);
      return super.open(path, bufferSize);
    }

    @Override
    public boolean delete(org.apache.hadoop.fs.Path path, boolean recursive) throws IOException {
      fail(StorageFailure.DELETE, path);
      return super.delete(path, recursive);
    }
  }

  private void useFaultInjectingFileSystem() throws IOException {
    storageConf.set("fs.file.impl", FaultInjectingFileSystem.class.getName());
    storageConf.set("fs.file.impl.disable.cache", "true");
    // Invalidate the earlier plain filesystem wrapper, then retain caching within this test: HFile
    // writers rely on the same wrapper instance to track bytes written during metadata bootstrap.
    storageConf.set("fs.hoodie-file.impl", org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem.class.getName());
    org.apache.hadoop.fs.FileSystem.get(java.net.URI.create("hoodie-file:///"),
        storageConf.unwrapAs(org.apache.hadoop.conf.Configuration.class)).close();
  }

  private void armStorageFailure(StorageFailure failure, String partition) {
    FaultInjectingFileSystem.INJECTIONS.set(0);
    FaultInjectingFileSystem.persistent = false;
    FaultInjectingFileSystem.targetPartition = partition;
    FaultInjectingFileSystem.failure = failure;
  }

  @ParameterizedTest
  @CsvSource({"CREATE,false", "CREATE,true", "WRITE,false", "WRITE,true", "CLOSE,false", "CLOSE,true"})
  public void testStorageWriteFailureCanRollbackAndRetry(StorageFailure failure, boolean persistent) throws Exception {
    useFaultInjectingFileSystem();
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      String failed = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, failed);
      armStorageFailure(failure, "par2");
      FaultInjectingFileSystem.persistent = persistent;
      boolean writeFailed = false;
      RuntimeException unexpectedFailure = null;
      try {
        List<WriteStatus> statuses = client.insert(Collections.singletonList(str2HoodieRecord("id2,failed,2,2,par2")), failed);
        writeFailed = statuses.stream().anyMatch(WriteStatus::hasErrors);
      } catch (RuntimeException e) {
        unexpectedFailure = e;
        writeFailed = true;
      } finally {
        FaultInjectingFileSystem.failure = null;
      }
      if (FaultInjectingFileSystem.INJECTIONS.get() == 0 && unexpectedFailure != null) {
        throw unexpectedFailure;
      }
      assertTrue(FaultInjectingFileSystem.INJECTIONS.get() > 0, "must exercise real data storage, not a fixture failure");
      assertFalse(HoodieTableMetaClient.reload(metaClient).getActiveTimeline().filterCompletedInstants().containsInstant(failed));
      assertEquals(Collections.singletonMap("id1", "id1,original,1,1,par1"), readAll(reader.readSnapshot()));
      assertTrue(client.rollback(failed));
      writeAndCommit(client, "id2,recovered,3,3,par2", true);
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,original,1,1,par1");
      expected.put("id2", "id2,recovered,3,3,par2");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      if (persistent) {
        assertTrue(writeFailed, "persistent data write failure must propagate an exception or erroneous write status");
      }
    } finally {
      FaultInjectingFileSystem.failure = null;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testReadIoFailureIsVisibleAndReaderCanRetry(boolean compactFirst) throws Exception {
    useFaultInjectingFileSystem();
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,value,1,1,par1", true);
      if (compactFirst) {
        compact(client);
      }
      armStorageFailure(StorageFailure.READ, "par1");
      FaultInjectingFileSystem.persistent = true;
      assertThrows(RuntimeException.class, () -> readAll(reader.readSnapshot()), "I/O errors must not become empty/partial successful results");
      assertTrue(FaultInjectingFileSystem.INJECTIONS.get() > 0);
      FaultInjectingFileSystem.failure = null;
      assertEquals(Collections.singletonMap("id1", "id1,value,1,1,par1"), readAll(reader.readSnapshot()));
    } finally {
      FaultInjectingFileSystem.failure = null;
    }
  }

  @Test
  public void testRollbackRetriesAfterDataFileDeleteFailure() throws Exception {
    useFaultInjectingFileSystem();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps()).withRollbackUsingMarkers(false).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      String failed = writeAndCommit(client, "id2,abandoned,2,2,par2", false);
      armStorageFailure(StorageFailure.DELETE, "par2");
      assertThrows(RuntimeException.class, () -> client.rollback(failed));
      assertEquals(1, FaultInjectingFileSystem.INJECTIONS.get());
      FaultInjectingFileSystem.failure = null;
      assertTrue(client.rollback(failed), "a requested/inflight rollback must be restartable");
      assertEquals(Collections.singletonMap("id1", "id1,original,1,1,par1"), readAll(reader.readSnapshot()));
      assertFalse(HoodieTableMetaClient.reload(metaClient).getActiveTimeline().getCommitsTimeline().containsInstant(failed));
      writeAndCommit(client, "id2,retry,3,3,par2", true);
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
    } finally {
      FaultInjectingFileSystem.failure = null;
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  public void testAbruptWriterProcessDeath(boolean compactFirst, boolean commitBeforeDeath) throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false)) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      if (compactFirst) {
        compact(client);
      }
    }
    String failed = WriteClientTestUtils.createNewInstantTime();
    List<String> command = new ArrayList<>();
    command.add(java.nio.file.Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
        .filter(arg -> arg.startsWith("--add-opens=") || arg.startsWith("--add-exports="))
        .forEach(command::add);
    command.add("-Xmx512m");
    command.add("-cp");
    command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
    command.add(AbruptWriter.class.getName());
    command.add(basePath);
    command.add(failed);
    command.add(Boolean.toString(commitBeforeDeath));
    Path log = tempDir.resolve("abrupt-writer.log");
    Process worker = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(worker.waitFor(60, TimeUnit.SECONDS), "child writer must reach the crash boundary within 60s: " + log);
      assertEquals(73, worker.exitValue(), () -> {
        try {
          return new String(Files.readAllBytes(log), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IOException e) {
          return e.toString();
        }
      });
    } finally {
      if (worker.isAlive()) {
        worker.destroyForcibly();
        assertTrue(worker.waitFor(10, TimeUnit.SECONDS));
      }
    }
    try (HoodieJavaWriteClient recovery = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      assertEquals(Collections.singletonMap("id1", commitBeforeDeath ? "id1,child,2,2,par1" : "id1,original,1,1,par1"),
          readAll(reader.readSnapshot()), "process death must not expose an incomplete delta commit");
      if (!commitBeforeDeath) {
        assertTrue(recovery.rollback(failed));
      }
      writeAndCommit(recovery, "id1,recovered,3,3,par1", true, WriteOperationType.UPSERT);
      compact(recovery);
      assertEquals(Collections.singletonMap("id1", "id1,recovered,3,3,par1"), readAll(reader.readOptimized()));
      assertEquals(1, readAllRecordKeys(reader.readSnapshot()).size());
    }
  }

  /** Separate JVM deliberately exits without close(), shutdown hooks, or heartbeat cleanup. */
  public static class AbruptWriter {
    public static void main(String[] args) throws Exception {
      TestHoodieJavaReadClient fixture = new TestHoodieJavaReadClient();
      fixture.basePath = args[0];
      fixture.storageConf = HoodieTestUtils.getDefaultStorageConf();
      fixture.context = new HoodieJavaEngineContext(fixture.storageConf, new TestJavaTaskContextSupplier());
      fixture.setUp();
      fixture.metaClient = HoodieTableMetaClient.builder().setConf(fixture.storageConf).setBasePath(args[0]).build();
      HoodieJavaWriteClient client = new HoodieJavaWriteClient(fixture.context, fixture.nbccBucketConfig());
      fixture.writeData(client, args[1], "id1,child,2,2,par1", Boolean.parseBoolean(args[2]), WriteOperationType.UPSERT);
      Runtime.getRuntime().halt(73);
    }
  }

  private static Stream<Arguments> tableVersionTransitions() {
    return Stream.of(6, 8, 9, 10).flatMap(from -> Stream.of(6, 8, 9, 10)
        .filter(to -> !from.equals(to)).map(to -> Arguments.of(from, to)));
  }

  /** Spark's version migration contract, starting with real Java MOR data instead of empty table metadata. */
  @ParameterizedTest
  @MethodSource("tableVersionTransitions")
  public void testTableVersionTransitionPreservesDataAndFurtherWrites(int from, int to) throws Exception {
    HoodieWriteConfig initial = HoodieWriteConfig.newBuilder().withProps(singleWriterInMemoryConfig().getProps())
        .withWriteTableVersion(from).withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, initial.getProps());
    Map<String, String> expected = Collections.singletonMap("id1", "id1,updated,2,2,par1");
    try (HoodieJavaWriteClient client = getHoodieWriteClient(initial, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(HoodieTableVersion.fromVersionCode(from), HoodieTableMetaClient.reload(metaClient).getTableConfig().getTableVersion());
    }
    HoodieWriteConfig target = HoodieWriteConfig.newBuilder().withProps(initial.getProps()).withWriteTableVersion(to)
        .withAutoUpgradeVersion(true).build();
    new UpgradeDowngrade(HoodieTableMetaClient.reload(metaClient), target, context, JavaUpgradeDowngradeHelper.getInstance())
        .run(HoodieTableVersion.fromVersionCode(to), null);
    metaClient = HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(basePath).build();
    assertEquals(HoodieTableVersion.fromVersionCode(to), metaClient.getTableConfig().getTableVersion());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(target, false);
         HoodieJavaReadClient reader = newReadClient()) {
      assertEquals(expected, readAll(reader.readSnapshot()), "migration must preserve merged values");
      String last = writeAndCommit(client, "id1,afterMigration,3,3,par1", true, WriteOperationType.UPSERT);
      expected = Collections.singletonMap("id1", "id1,afterMigration,3,3,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      if (to >= 8) {
        assertEquals(expected, readAll(reader.readIncremental(completionTimeOf(last), completionTimeOf(last))));
      }
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(1, readAllRecordKeys(reader.readSnapshot()).size());
    }
  }

  @ParameterizedTest
  @CsvSource({"PARQUET,avro", "PARQUET,parquet", "PARQUET,hfile", "ORC,avro", "ORC,parquet", "ORC,hfile",
      "HFILE,avro", "HFILE,parquet", "HFILE,hfile"})
  public void testLegacyVersionSixStorageLifecycle(String baseFormat, String logFormat) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(singleWriterInMemoryConfig().getProps())
        .withWriteTableVersion(6).withBaseFileFormat(baseFormat)
        .withProps(Collections.singletonMap(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFormat))
        .withStorageConfig(HoodieStorageConfig.newBuilder().logFileDataBlockFormat(logFormat).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
      Map<String, String> expected = Collections.singletonMap("id1", "id1,updated,2,2,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      String deleted = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, deleted);
      List<WriteStatus> statuses = client.delete(new ArrayList<>(Collections.singletonList(new HoodieKey("id1", "par1"))), deleted);
      assertNoErrors(statuses);
      commit(client, deleted, statuses);
      assertTrue(readAll(reader.readSnapshot()).isEmpty());
      assertTrue(client.rollback(deleted));
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(HoodieTableVersion.SIX, HoodieTableMetaClient.reload(metaClient).getTableConfig().getTableVersion());
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageFailure.class, names = {"CREATE", "WRITE", "DELETE"})
  public void testRollbackRecoversAfterMetadataStorageFailure(StorageFailure failure) throws Exception {
    useFaultInjectingFileSystem();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build()).withRollbackUsingMarkers(false)
        .withHeartbeatIntervalInMs(1000).withHeartbeatTolerableMisses(2).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    String abandoned;
    try (FailingCommitClient client = new FailingCommitClient(config)) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      abandoned = WriteClientTestUtils.createNewInstantTime();
      List<WriteStatus> statuses = writeData(client, abandoned, "id2,abandoned,2,2,par2", false);
      client.failurePhase = CommitFailurePhase.AFTER_METADATA;
      assertThrows(IllegalStateException.class, () -> commit(client, abandoned, statuses));
      assertEquals(1, client.injectedFailures);
    }
    try (HoodieJavaWriteClient recovery = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      armStorageFailure(failure, "metadata");
      FaultInjectingFileSystem.persistent = true;
      boolean rollbackFailed = false;
      try {
        rollbackFailed = !recovery.rollback(abandoned);
      } catch (RuntimeException e) {
        rollbackFailed = true;
      } finally {
        FaultInjectingFileSystem.failure = null;
      }
      assertTrue(FaultInjectingFileSystem.INJECTIONS.get() > 0, "rollback must attempt a real metadata-table storage operation");
      if (rollbackFailed) {
        // A failed DELETE can leave the stopped rollback owner's heartbeat on storage. A new MDT
        // writer must wait for that lease to expire; immediate reclamation would race a live owner.
        String metadataPath = basePath + "/.hoodie/metadata";
        HoodieTableMetaClient metadataTable = HoodieTableMetaClient.builder().setConf(storageConf.newInstance())
            .setBasePath(metadataPath).build();
        org.awaitility.Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() ->
            metadataTable.reloadActiveTimeline().getRollbackTimeline().filterInflights().getInstantsAsStream()
                .allMatch(instant -> {
                  try {
                    long heartbeat = org.apache.hudi.common.heartbeat.HoodieHeartbeatUtils.getLastHeartbeatTime(
                        metadataTable.getStorage(), metadataPath, instant.requestedTime());
                    return System.currentTimeMillis() - heartbeat > config.getHoodieClientHeartbeatIntervalInMs()
                        * config.getHoodieClientHeartbeatTolerableMisses();
                  } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                  }
                }));
        assertTrue(recovery.rollback(abandoned));
      }
      Map<String, String> expected = Collections.singletonMap("id1", "id1,original,1,1,par1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      writeAndCommit(recovery, "id2,recovered,3,3,par2", true);
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
      TypedProperties filesystem = new TypedProperties();
      filesystem.setProperty(HoodieMetadataConfig.ENABLE.key(), "false");
      try (HoodieJavaReadClient filesystemReader = new HoodieJavaReadClient(context, basePath, filesystem)) {
        assertEquals(readAll(filesystemReader.readSnapshot()), readAll(reader.readSnapshot()));
      }
      assertTrue(rollbackFailed, "persistent metadata storage failure must not acknowledge a completed rollback");
    } finally {
      FaultInjectingFileSystem.failure = null;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testReadFailureAfterPartialResultsClosesSpillAndAllowsRetry(boolean compactFirst) throws Exception {
    useFaultInjectingFileSystem();
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    Path spill = tempDir.resolve("partial-failure-spill");
    Files.createDirectories(spill);
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), "1");
    props.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), spill.toString());
    props.setProperty(HoodieMetadataConfig.ENABLE.key(), "false");
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = new HoodieJavaReadClient(context, basePath, props)) {
      writeAndCommit(client, "id1,original,1,1,par1", true);
      writeAndCommit(client, "id2,original,1,1,par2", true);
      if (compactFirst) {
        compact(client);
      }
      writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
      writeAndCommit(client, "id2,updated,2,2,par2", true, WriteOperationType.UPSERT);
      try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator = reader.readSnapshot()) {
        HoodieRecord<IndexedRecord> first = iterator.next();
        try (Stream<Path> files = Files.list(spill)) {
          assertTrue(files.findAny().isPresent(), "the first file-group buffer must actually spill");
        }
        String unreadPartition = "id1".equals(first.getRecordKey()) ? "par2" : "par1";
        armStorageFailure(StorageFailure.READ, unreadPartition);
        FaultInjectingFileSystem.persistent = true;
        assertThrows(RuntimeException.class, iterator::hasNext,
            "a later file-group failure must reach the caller after partial results");
        assertTrue(FaultInjectingFileSystem.INJECTIONS.get() > 0);
      } finally {
        FaultInjectingFileSystem.failure = null;
      }
      try (Stream<Path> files = Files.list(spill)) {
        assertEquals(0, files.count(), "failed file-group initialization must not leak the earlier spill buffer");
      }
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,updated,2,2,par1");
      expected.put("id2", "id2,updated,2,2,par2");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
    } finally {
      FaultInjectingFileSystem.failure = null;
    }
  }

  /** OCC must reject overlapping file groups and preserve both commits for independent groups. */
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  public void testOptimisticConcurrentWritersPreserveCommittedRows(boolean compactFirst, boolean overlapping) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(nbccBucketConfig().getProps())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new SimpleConcurrentFileWritesConflictResolutionStrategy()).build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient seed = getHoodieWriteClient(nbccBucketConfig(), false)) {
      writeAndCommit(seed, "id1,original,1,1,par1", true);
      writeAndCommit(seed, "id2,original,1,1,par2", true);
      if (compactFirst) {
        compact(seed);
      }
    }
    try (HoodieJavaWriteClient first = new HoodieJavaWriteClient(context, config);
         HoodieJavaWriteClient second = new HoodieJavaWriteClient(context, config);
         HoodieJavaReadClient reader = newReadClient()) {
      String firstInstant = WriteClientTestUtils.createNewInstantTime();
      List<WriteStatus> firstStatuses = writeData(first, firstInstant, "id1,first,2,2,par1", false, WriteOperationType.UPSERT);
      String secondInstant = WriteClientTestUtils.createNewInstantTime();
      String secondRow = overlapping ? "id1,second,3,3,par1" : "id2,second,3,3,par2";
      List<WriteStatus> secondStatuses = writeData(second, secondInstant, secondRow, false, WriteOperationType.UPSERT);
      commit(first, firstInstant, firstStatuses);
      if (overlapping) {
        assertThrows(HoodieWriteConflictException.class, () -> commit(second, secondInstant, secondStatuses));
      } else {
        commit(second, secondInstant, secondStatuses);
      }
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,first,2,2,par1");
      expected.put("id2", overlapping ? "id2,original,1,1,par2" : "id2,second,3,3,par2");
      assertEquals(expected, readAll(reader.readSnapshot()), "uncommitted conflicting logs must not overwrite the winner");
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
      assertEquals(!overlapping, HoodieTableMetaClient.reload(metaClient).getActiveTimeline().filterCompletedInstants().containsInstant(secondInstant));
      if (overlapping) {
        assertTrue(second.rollback(secondInstant));
        writeAndCommit(second, "id1,retry,4,4,par1", true, WriteOperationType.UPSERT);
        expected.put("id1", "id1,retry,4,4,par1");
      }
      compact(second);
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(expected, readAll(reader.readSnapshot()));
    }
  }

  private HoodieWriteConfig singleWriterInMemoryConfig() {
    Properties props = new Properties();
    props.putAll(nbccBucketConfig().getProps());
    props.remove(HoodieLayoutConfig.LAYOUT_TYPE.key());
    props.remove(HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME.key());
    return HoodieWriteConfig.newBuilder().withProps(props)
        .withWriteConcurrencyMode(WriteConcurrencyMode.SINGLE_WRITER)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .build();
  }

  /** Exercise every built-in index exposed by JavaHoodieIndexFactory on an actual MOR table. */
  @ParameterizedTest
  @EnumSource(value = HoodieIndex.IndexType.class, names = {"SIMPLE", "GLOBAL_SIMPLE", "INMEMORY", "BLOOM", "BUCKET"})
  public void testMorWriteReadLifecycleWithEachJavaIndex(HoodieIndex.IndexType indexType) throws Exception {
    Properties props = new Properties();
    props.putAll(singleWriterInMemoryConfig().getProps());
    // Recompute the layout for the selected index instead of inheriting INMEMORY's default layout.
    props.remove(HoodieLayoutConfig.LAYOUT_TYPE.key());
    props.remove(HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME.key());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withEngineType(EngineType.JAVA).withProps(props)
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(getPropertiesForKeyGen(true))
            .withIndexType(indexType).withBucketNum("1").build()).build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config, false);
         HoodieJavaReadClient reader = newReadClient()) {
      writeAndCommit(client, "id1,first,1,1,par1", true);
      writeAndCommit(client, "id2,untouched,1,1,par2", true);
      writeAndCommit(client, "id1,updated,2,2,par1", true, WriteOperationType.UPSERT);
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,updated,2,2,par1");
      expected.put("id2", "id2,untouched,1,1,par2");
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
      compact(client);
      assertEquals(expected, readAll(reader.readOptimized()));
      String delete = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, delete);
      List<WriteStatus> statuses = client.delete(new ArrayList<>(Collections.singletonList(new HoodieKey("id1", "par1"))), delete);
      assertNoErrors(statuses);
      commit(client, delete, statuses);
      expected.remove("id1");
      assertEquals(expected, readAll(reader.readSnapshot()));
      writeAndCommit(client, "id1,returned,3,3,par1", true, WriteOperationType.UPSERT);
      expected.put("id1", "id1,returned,3,3,par1");
      compact(client);
      assertEquals(expected, readAll(reader.readSnapshot()));
      assertEquals(expected, readAll(reader.readOptimized()));
      assertEquals(2, readAllRecordKeys(reader.readSnapshot()).size());
    }
  }

  private HoodieWriteConfig nbccBucketConfig() {
    return nbccBucketConfig(RecordMergeMode.CUSTOM, OverwriteWithLatestAvroPayload.class.getName());
  }

  private HoodieWriteConfig nbccBucketConfig(RecordMergeMode recordMergeMode, String payloadClassName) {
    Properties props = getPropertiesForKeyGen(true);
    props.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    props.put(ENABLE_SCHEMA_CONFLICT_RESOLUTION.key(), "false");
    return HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withProps(Collections.singletonMap(HoodieTableConfig.ORDERING_FIELDS.key(), "ts"))
        .forTable("test")
        .withPath(basePath)
        .withSchema(jsonSchema)
        .withRecordMergeMode(recordMergeMode)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadClass(payloadClassName).build())
        // Inline/auto compaction is never enabled in these tests (compaction only ever runs via the
        // explicit compact() helper below), so this threshold only controls whether scheduleCompaction()
        // itself is willing to produce a plan on demand -- keep it at 1 (like TestJavaNonBlockingConcurrencyControl)
        // so compact() can always be called after at least one delta commit.
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketNum("1")
            .build())
        .withPopulateMetaFields(true)
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL)
        .withMarkersType(MarkerType.DIRECT.name())
        .withEmbeddedTimelineServerEnabled(false)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new BucketIndexConcurrentFileWritesConflictResolutionStrategy())
            .build())
        .build();
  }

  /**
   * Writes a single new-key record (INSERT) and, when {@code doCommit}, commits it; returns the instant's
   * requested time. Use the {@link WriteOperationType} overload for updates to an already-written key.
   */
  private String writeAndCommit(HoodieJavaWriteClient client, String record, boolean doCommit) throws IOException {
    return writeAndCommit(client, record, doCommit, WriteOperationType.INSERT);
  }

  private String writeAndCommit(HoodieJavaWriteClient client, String record, boolean doCommit, WriteOperationType opType) throws IOException {
    String instant = WriteClientTestUtils.createNewInstantTime();
    writeData(client, instant, record, doCommit, opType);
    return instant;
  }

  private String writeEventTimeAndCommit(HoodieJavaWriteClient client, String record,
                                         WriteOperationType opType) throws IOException {
    GenericRecord genericRecord = str2GenericRecord(record);
    HoodieRecord hoodieRecord = new HoodieAvroRecord<>(
        new HoodieKey((String) genericRecord.get("id"), (String) genericRecord.get("part")),
        new EventTimeAvroPayload(genericRecord, (Long) genericRecord.get("ts")));
    String instant = WriteClientTestUtils.createNewInstantTime();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> statuses = opType == WriteOperationType.INSERT
        ? client.insert(Collections.singletonList(hoodieRecord), instant)
        : client.upsert(Collections.singletonList(hoodieRecord), instant);
    assertNoErrors(statuses);
    commit(client, instant, statuses);
    return instant;
  }

  private List<WriteStatus> writeData(HoodieJavaWriteClient client, String instant, String record, boolean doCommit) throws IOException {
    return writeData(client, instant, record, doCommit, WriteOperationType.INSERT);
  }

  private List<WriteStatus> writeData(HoodieJavaWriteClient client, String instant, String record, boolean doCommit,
                                      WriteOperationType opType) throws IOException {
    List<HoodieRecord> records = Collections.singletonList(str2HoodieRecord(record));
    metaClient = HoodieTableMetaClient.reload(metaClient);
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> statuses;
    switch (opType) {
      case INSERT:
        statuses = client.insert(records, instant);
        break;
      case UPSERT:
        statuses = client.upsert(records, instant);
        break;
      default:
        throw new UnsupportedOperationException(opType + " is not supported in this test helper");
    }
    assertNoErrors(statuses);
    if (doCommit) {
      commit(client, instant, statuses);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return statuses;
  }

  private void commit(HoodieJavaWriteClient client, String instant, List<WriteStatus> statuses) {
    List<HoodieWriteStat> writeStats =
        statuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
    boolean committed = client.commitStats(instant, writeStats, Option.empty(), metaClient.getCommitActionType());
    assertTrue(committed);
    metaClient = HoodieTableMetaClient.reload(metaClient);
  }

  /**
   * @return the compaction instant's requested time.
   */
  private String compact(HoodieJavaWriteClient client) {
    String compactionTime = (String) client.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client.compact(compactionTime);
    client.commitCompaction(compactionTime, writeMetadata, Option.empty());
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));
    return compactionTime;
  }

  private String completionTimeOf(String requestedTime) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant instant = metaClient.getActiveTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(i -> i.requestedTime().equals(requestedTime))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No completed instant found with requested time " + requestedTime));
    return instant.getCompletionTime();
  }

  private static void assertNoErrors(List<WriteStatus> statuses) {
    for (WriteStatus status : statuses) {
      assertFalse(status.hasErrors(), "Errors found in write of partition " + status.getPartitionPath());
    }
  }

  private GenericRecord str2GenericRecord(String str) {
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    String[] fieldValues = str.split(",", -1);
    ValidationUtils.checkArgument(fieldValues.length == 5, "Valid record must have 5 fields");
    record.put("id", StringUtils.isNullOrEmpty(fieldValues[0]) ? null : fieldValues[0]);
    record.put("name", StringUtils.isNullOrEmpty(fieldValues[1]) ? null : fieldValues[1]);
    record.put("age", StringUtils.isNullOrEmpty(fieldValues[2]) ? null : Integer.parseInt(fieldValues[2]));
    record.put("ts", StringUtils.isNullOrEmpty(fieldValues[3]) ? null : Long.parseLong(fieldValues[3]));
    record.put("part", StringUtils.isNullOrEmpty(fieldValues[4]) ? null : fieldValues[4]);
    return record;
  }

  private HoodieRecord str2HoodieRecord(String str) {
    GenericRecord record = str2GenericRecord(str);
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(record, (Long) record.get("ts"));
    return new HoodieAvroRecord<>(new HoodieKey((String) record.get("id"), (String) record.get("part")), payload);
  }

  /**
   * Reads every record into a map keyed by record key, valued by a comparable summary of its fields
   * ("id,name,age,ts,part"), and closes the iterator. Because this collects into a Map, a record key
   * appearing more than once (e.g. a duplicate/undercounted row) is invisible here -- use
   * {@link #readAllRecordKeys} when a test needs to assert on occurrence counts, not just presence.
   */
  private Map<String, String> readAll(ClosableIterator<HoodieRecord<IndexedRecord>> iterator) {
    Map<String, String> result = new HashMap<>();
    try {
      while (iterator.hasNext()) {
        HoodieRecord<IndexedRecord> record = iterator.next();
        GenericRecord generic = (GenericRecord) record.getData();
        result.put(record.getRecordKey(), summarize(generic));
      }
    } finally {
      iterator.close();
    }
    return result;
  }

  /**
   * Reads every record's key into a List, preserving duplicates, and closes the iterator.
   */
  private List<String> readAllRecordKeys(ClosableIterator<HoodieRecord<IndexedRecord>> iterator) {
    List<String> keys = new ArrayList<>();
    try {
      while (iterator.hasNext()) {
        keys.add(iterator.next().getRecordKey());
      }
    } finally {
      iterator.close();
    }
    return keys;
  }

  private static String summarize(GenericRecord record) {
    return String.join(",",
        String.valueOf(record.get("id")),
        String.valueOf(record.get("name")),
        String.valueOf(record.get("age")),
        String.valueOf(record.get("ts")),
        String.valueOf(record.get("part")));
  }
}
