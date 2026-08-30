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

import org.apache.hudi.client.transaction.BucketIndexConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.TYPE;
import static org.apache.hudi.config.HoodieWriteConfig.ENABLE_SCHEMA_CONFLICT_RESOLUTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieJavaReadClient}: snapshot, read-optimized, time-travel, and completion-time-based
 * incremental reads on the Java engine, including NBCC (Non-Blocking Concurrency Control) read-side
 * correctness -- this is the read-side validation for the NBCC write support in
 * {@code TestJavaNonBlockingConcurrencyControl}.
 *
 * <p>CDC (change-data-capture) reads are out of scope: they require CDC-enabled writes, which the Java
 * engine's write path does not yet produce.
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

  private HoodieWriteConfig nbccBucketConfig() {
    Properties props = getPropertiesForKeyGen(true);
    props.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    props.put(ENABLE_SCHEMA_CONFLICT_RESOLUTION.key(), "false");
    return HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withProps(Collections.singletonMap(HoodieTableConfig.ORDERING_FIELDS.key(), "ts"))
        .forTable("test")
        .withPath(basePath)
        .withSchema(jsonSchema)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadClass(OverwriteWithLatestAvroPayload.class.getName()).build())
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
