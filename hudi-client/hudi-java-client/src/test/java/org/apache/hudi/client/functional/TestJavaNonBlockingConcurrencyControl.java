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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.BucketIndexConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.JavaBucketIndexPartitioner;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.TYPE;
import static org.apache.hudi.config.HoodieWriteConfig.ENABLE_SCHEMA_CONFLICT_RESOLUTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests non-blocking concurrency control (NBCC) for MERGE_ON_READ tables on the Java engine, ported from
 * {@code org.apache.hudi.table.functional.TestSparkNonBlockingConcurrencyControl}.
 *
 * <p>bulk_insert is bucket-index-aware on the Java engine via {@code JavaBucketIndexBulkInsertPartitioner}
 * (mirroring Spark's {@code RDDSimpleBucketBulkInsertPartitioner}): it appends to an existing bucket's file
 * group, or, for a brand-new bucket, creates a new base file with a fixed (bucket-id-derived) fileId under
 * NBCC so that concurrent writers touching the same new bucket are recognized as conflicting by
 * {@code BucketIndexConcurrentFileWritesConflictResolutionStrategy}. Unlike INSERT/UPSERT, BULK_INSERT is the
 * one operation under NBCC that still goes through conflict resolution ({@code
 * HoodieWriteConfig#needResolveWriteConflict}), so concurrent bulk_inserts (or a bulk_insert concurrent with
 * an insert/upsert) can fail with {@code HoodieWriteConflictException} depending on completion order -- see
 * the {@code testBulkInsertAndInsertConcurrentCaseN} tests below.</p>
 */
@Tag("functional")
public class TestJavaNonBlockingConcurrencyControl extends HoodieJavaClientTestHarness {

  private final String jsonSchema = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_commit_seqno\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_record_key\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_partition_path\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_file_name\", \"type\": [\"null\", \"string\"]},\n"
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
    // metaClient is (re)initialized per test, once the test-specific HoodieWriteConfig is built,
    // so that the table properties (record key/partition fields, ordering field) match the config.
  }

  @BeforeEach
  public void setUp() throws Exception {
    schema = HoodieSchema.parse(jsonSchema);
  }

  @Test
  public void testNonBlockingConcurrencyControlWithPartialUpdatePayload() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // start the 2nd txn and insert record: [id1,null,23,2,par1], suspend the tx commit
    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.INSERT);

    // step to commit the 1st txn
    client1.commitStats(insertTime1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // step to commit the 2nd txn
    client2.commitStats(insertTime2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // There is no base file in partition dir because there is no compaction yet.
    assertFalse(baseDataFileExists(), "No base data files should have been created");

    // do compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    // result is [(id1,Danny,23,2,par1)]
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  // The core NBCC stress: the 2nd txn STARTS after the 1st, but COMPLETES (commits) before it. NBCC must
  // not conflict on this, and the compacted result must reflect both non-conflicting writes.
  @Test
  public void testNonBlockingConcurrencyControlOutOfOrderCompletion() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // start the 2nd txn AFTER the 1st and insert record: [id1,null,23,2,par1], suspend the tx commit
    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.INSERT);
    assertTrue(insertTime2.compareTo(insertTime1) > 0, "the 2nd txn must have started after the 1st");

    // out-of-order completion: the later-started 2nd txn commits FIRST
    client2.commitStats(insertTime2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());
    // the earlier-started 1st txn commits LAST -- NBCC must not conflict on this
    client1.commitStats(insertTime1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // both commits landed, with the 2nd txn's completion time earlier than the 1st's, proving completion
    // order was allowed to differ from start order without conflict
    metaClient.reloadActiveTimeline();
    HoodieInstant instant1 = metaClient.getActiveTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(i -> i.requestedTime().equals(insertTime1)).findFirst().get();
    HoodieInstant instant2 = metaClient.getActiveTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(i -> i.requestedTime().equals(insertTime2)).findFirst().get();
    assertTrue(instant2.getCompletionTime().compareTo(instant1.getCompletionTime()) < 0,
        "the later-started txn must have completed before the earlier-started one");

    // do compaction and verify the merged result reflects data from both non-conflicting writers
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    // result is [(id1,Danny,23,2,par1)]
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  @Test
  public void testNonBlockingConcurrencyControlWithInflightInstant() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // start the 2nd txn and insert record: [id1,null,23,2,par1], suspend the tx commit
    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    writeData(client2, insertTime2, dataset2, false, WriteOperationType.INSERT);

    // step to commit the 1st txn
    client1.commitStats(insertTime1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // schedule compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();

    // step to commit the 3rd txn, insert record: [id3,Julian,53,4,par1] and commit 3rd txn
    List<String> dataset3 = Collections.singletonList("id3,Julian,53,4,par1");
    String insertTime3 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses3 = writeData(client1, insertTime3, dataset3, false, WriteOperationType.INSERT);
    client1.commitStats(insertTime3, writeStatuses3.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // do compaction
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    // read optimized result is [(id1,Danny,23,1,par1)] because the 2nd commit (client2) is still
    // inflight and the data belonging to the 3rd commit is not included in the compaction plan scheduled earlier.
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,null,1,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  // Validate that multiple writers will only produce base files for bulk insert; every other write into
  // that bucket under NBCC -- including the very first regular INSERT/UPSERT into a brand-new bucket --
  // goes straight to a log file (BaseSparkBucketIndexBucketInfoGetter semantics: "always write into log
  // file instead of base file if using NB-CC"). Also verifies the merged result after compaction.
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMultiBaseFile(boolean bulkInsertFirst) throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig(true);
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    // there should only be a single filegroup, so we will verify that it is consistent
    String fileID = null;

    // if there is not a bulk insert first, then we will write to log files for a filegroup without a base
    // file. Having a base file adds the possibility of small file handling which we want to ensure doesn't
    // happen.
    if (bulkInsertFirst) {
      HoodieJavaWriteClient client0 = getHoodieWriteClient(config, false);
      List<String> dataset0 = Collections.singletonList("id0,Danny,0,0,par1");
      String insertTime0 = WriteClientTestUtils.createNewInstantTime();
      List<WriteStatus> writeStatuses0 = writeData(client0, insertTime0, dataset0, false, WriteOperationType.BULK_INSERT, true);
      client0.commitStats(insertTime0, writeStatuses0.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType());
      for (WriteStatus status : writeStatuses0) {
        if (fileID == null) {
          fileID = status.getFileId();
        } else {
          assertEquals(fileID, status.getFileId());
        }
        assertFalse(FSUtils.isLogFile(new StoragePath(status.getStat().getPath()).getName()));
      }
      client0.close();
    }

    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);
    List<String> dataset1 = Collections.singletonList("id1,Danny,22,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT, true);
    assertFalse(writeStatuses1.isEmpty(), "insert into a fresh bucket must produce at least one write status");
    for (WriteStatus status : writeStatuses1) {
      if (fileID == null) {
        fileID = status.getFileId();
      } else {
        assertEquals(fileID, status.getFileId());
      }
      assertTrue(FSUtils.isLogFile(new StoragePath(status.getStat().getPath()).getName()));
    }

    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    List<String> dataset2 = Collections.singletonList("id1,Danny,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.UPSERT, true);
    assertFalse(writeStatuses2.isEmpty(), "upsert into an existing bucket must produce at least one write status");
    for (WriteStatus status : writeStatuses2) {
      assertEquals(fileID, status.getFileId());
      assertTrue(FSUtils.isLogFile(new StoragePath(status.getStat().getPath()).getName()));
    }

    client1.commitStats(insertTime1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());
    client2.commitStats(insertTime2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // Prove the writers genuinely overlapped in time, so this test cannot pass merely because they happened
    // to run serialized (ported from TestSparkNonBlockingConcurrencyControl#testMultiBaseFile).
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> instants = metaClient.getActiveTimeline().getInstants();
    if (bulkInsertFirst) {
      assertEquals(3, instants.size());
      // the bulk_insert finished before the insert started
      assertTrue(Long.parseLong(instants.get(0).getCompletionTime()) < Long.parseLong(instants.get(1).requestedTime()));
      // the insert and upsert overlapped in time
      assertTrue(Long.parseLong(instants.get(1).getCompletionTime()) > Long.parseLong(instants.get(2).requestedTime()));
      assertTrue(Long.parseLong(instants.get(2).getCompletionTime()) > Long.parseLong(instants.get(1).requestedTime()));
    } else {
      assertEquals(2, instants.size());
      // the insert and upsert overlapped in time
      assertTrue(Long.parseLong(instants.get(0).getCompletionTime()) > Long.parseLong(instants.get(1).requestedTime()));
      assertTrue(Long.parseLong(instants.get(1).getCompletionTime()) > Long.parseLong(instants.get(0).requestedTime()));
    }

    // compact and verify the merged (later-completing) value survives
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    // when bulk_insert ran first, its id0 record shares the same (single) bucket and survives compaction
    // alongside the merged id1 record
    String expected = bulkInsertFirst
        ? "[id0,par1,id0,Danny,0,0,par1, id1,par1,id1,Danny,23,2,par1]"
        : "[id1,par1,id1,Danny,23,2,par1]";
    Map<String, String> result = Collections.singletonMap("par1", expected);
    checkWrittenData(result, 1);

    client1.close();
    client2.close();
  }

  /**
   * case1: insert commits before bulk_insert starts. Both txns succeed.
   * <pre>
   *  |------ txn1: insert ------|
   *                             |------ txn2: bulk_insert ------|
   * </pre>
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase1() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());

    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    writeData(client1, insertTime1, dataset1, true, WriteOperationType.INSERT);

    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    writeData(client2, insertTime2, dataset2, true, WriteOperationType.BULK_INSERT);

    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  /**
   * case2: insert and bulk_insert overlap in time, with insert completing first.
   * <pre>
   *  |------ txn1: insert ------|
   *                      |------ txn2: bulk_insert ------|
   * </pre>
   * txn2 (bulk_insert) must fail to commit due to conflict: it is the operation that goes through
   * conflict resolution under NBCC, and it completed after a concurrent commit (txn1) touching the same
   * (brand-new) bucket.
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase2() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);

    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.BULK_INSERT);

    client1.commitStats(insertTime1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    HoodieWriteConflictException conflict = assertThrows(HoodieWriteConflictException.class, () -> client2.commitStats(insertTime2,
        writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType()));
    // assert the message identifies the actual conflicting operations, not just any HoodieWriteConflictException,
    // so an unrelated future conflict can't keep this test green.
    assertConflictMessageIdentifiesOperations(conflict, WriteOperationType.BULK_INSERT, WriteOperationType.INSERT);
    client1.close();
    client2.close();
  }

  /**
   * case3: bulk_insert and insert overlap in time, with bulk_insert starting first but insert completing
   * first.
   * <pre>
   *          |------ txn2: insert ------|
   *    |---------- txn1: bulk_insert ----------|
   * </pre>
   * txn1 (bulk_insert) must fail to commit: same outcome as case2 (whichever op is bulk_insert conflicts
   * when it completes after a concurrent commit), just with start order swapped -- proving conflict
   * detection is based on completion order, not start order.
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase3() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);

    // start the 1st txn and bulk insert record, suspend the tx commit
    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.BULK_INSERT);

    // start the 2nd txn and insert record, suspend the tx commit
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // step to commit the 2nd txn (insert) first
    client1.commitStats(insertTime1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // step to commit the 1st txn (bulk_insert) last
    HoodieWriteConflictException conflict = assertThrows(HoodieWriteConflictException.class, () -> client2.commitStats(insertTime2,
        writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType()));
    // assert the message identifies the actual conflicting operations, not just any HoodieWriteConflictException,
    // so an unrelated future conflict can't keep this test green.
    assertConflictMessageIdentifiesOperations(conflict, WriteOperationType.BULK_INSERT, WriteOperationType.INSERT);
    client1.close();
    client2.close();
  }

  /**
   * case4: insert starts first, bulk_insert starts second, but bulk_insert completes first.
   * <pre>
   *  |------------ txn1: insert ------------|
   *      |------ txn2: bulk_insert ------|
   * </pre>
   * Both txns succeed: bulk_insert has no concurrent completed commit to conflict against yet, and the
   * (non-bulk_insert) insert never goes through conflict resolution under NBCC.
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase4() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);

    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.BULK_INSERT);

    // step to commit the 2nd txn (bulk_insert) first
    client2.commitStats(insertTime2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // step to commit the 1st txn (insert) last
    client1.commitStats(insertTime1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  /**
   * case5: bulk_insert starts first, insert starts second, but bulk_insert completes first.
   * <pre>
   *                          |------ txn2: insert ------|
   *    |---------- txn1: bulk_insert ----------|
   * </pre>
   * Both txns succeed: same outcome as case4, just with start order swapped.
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase5() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);

    // start the 1st txn and bulk insert record, suspend the tx commit
    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.BULK_INSERT);

    // start the 2nd txn and insert record, suspend the tx commit
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // step to commit the 1st txn (bulk_insert) first
    client2.commitStats(insertTime2, writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    // step to commit the 2nd txn (insert) last
    client1.commitStats(insertTime1, writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType());

    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  /**
   * case6: bulk_insert commits before insert starts. Both txns succeed.
   * <pre>
   *                                   |------ txn2: insert ------|
   *  |------ txn1: bulk_insert ------|
   * </pre>
   */
  @Test
  public void testBulkInsertAndInsertConcurrentCase6() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());

    HoodieJavaWriteClient client1 = getHoodieWriteClient(config, false);
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = WriteClientTestUtils.createNewInstantTime();
    writeData(client1, insertTime1, dataset1, true, WriteOperationType.BULK_INSERT);

    HoodieJavaWriteClient client2 = getHoodieWriteClient(config, false);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = WriteClientTestUtils.createNewInstantTime();
    writeData(client2, insertTime2, dataset2, true, WriteOperationType.INSERT);

    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client1.compact(compactionTime);
    client1.commitCompaction(compactionTime, writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));

    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
    client1.close();
    client2.close();
  }

  @Test
  public void testNonBlockingConcurrencyControlRequiresMergeOnRead() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
        baseNbccConfigBuilder(HoodieTableType.COPY_ON_WRITE).build());
    assertTrue(e.getMessage().contains("Non-blocking concurrency control requires the MOR table with simple "
        + "bucket index or it has to be Metadata table"));
  }

  @Test
  public void testNonBlockingConcurrencyControlRequiresBucketIndex() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
        HoodieWriteConfig.newBuilder()
            .withEngineType(EngineType.JAVA)
            .withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
            .forTable("nbcc-non-bucket")
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build())
            .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
            .withWriteConcurrencyMode(WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL)
            .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
            .build());
    assertTrue(e.getMessage().contains("Non-blocking concurrency control requires the MOR table with simple "
        + "bucket index or it has to be Metadata table"));
  }

  // HoodieWriteConfig.Builder#setDefaults auto-corrects hoodie.clean.failed.writes.policy to LAZY for any
  // multi-writer concurrency mode (including NBCC) before validate() runs, so an explicit EAGER request is
  // silently overridden rather than rejected; the checkArgument in validate() guarding this is unreachable
  // through the public builder API. Assert the actual (safety-net) behavior instead.
  @Test
  public void testNonBlockingConcurrencyControlAutoCorrectsEagerCleaningToLazy() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .forTable("nbcc-eager-clean")
        .withProps(Collections.singletonMap(TYPE.key(), HoodieTableType.MERGE_ON_READ.name()))
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
            .withBucketNum("1")
            .withRecordKeyField("_row_key")
            .withIndexKeyField("_row_key")
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .build();
    assertEquals(HoodieFailedWritesCleaningPolicy.LAZY.name(), config.getFailedWritesCleanPolicy().name());
  }

  /**
   * Single-writer, non-NBCC bucket-index MOR test: proves {@link JavaBucketIndexPartitioner} routes records
   * to the right bucket across multiple partitions and multiple buckets, that a subsequent upsert lands in the
   * same bucket's file group (rather than creating a new file group), and that compaction still succeeds.
   */
  @Test
  public void testBucketIndexRoutingAcrossPartitionsAndBuckets() throws Exception {
    int numBuckets = 4;
    // seed the builder with the SIMPLE index type: the bucket-specific index config below fully replaces
    // it before build(), we only need a valid placeholder here (BUCKET would fail this inner build() for
    // lacking a hash field/bucket count, since it is validated eagerly by HoodieIndexConfig.Builder#build)
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE,
        HoodieFailedWritesCleaningPolicy.EAGER)
        .withLayoutConfig(HoodieLayoutConfig.newBuilder()
            .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
            .withLayoutPartitioner(JavaBucketIndexPartitioner.class.getName())
            .build())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
            .withBucketNum(String.valueOf(numBuckets))
            .withRecordKeyField("_row_key")
            .withIndexKeyField("_row_key")
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 100);
    WriteClientTestUtils.startCommitWithTime(client, insertTime);
    List<WriteStatus> insertStatuses = client.insert(inserts, insertTime);
    assertNoErrors(insertStatuses);
    client.commit(insertTime, insertStatuses);

    // every base file's fileId bucket-prefix must match the hash of every record key routed into it, and a
    // given (partition, bucket) pair must only ever be represented by a single fileId
    Map<String, String> partitionBucketToFileId = new HashMap<>();
    for (WriteStatus status : insertStatuses) {
      int bucketIdFromFile = BucketIdentifier.bucketIdFromFileId(status.getFileId());
      assertTrue(bucketIdFromFile >= 0 && bucketIdFromFile < numBuckets);
      String partitionBucketKey = status.getPartitionPath() + "#" + bucketIdFromFile;
      String existing = partitionBucketToFileId.putIfAbsent(partitionBucketKey, status.getFileId());
      assertTrue(existing == null || existing.equals(status.getFileId()),
          "a single (partition, bucket) must map to a single file group");
      status.getWrittenRecordDelegates().forEach(delegate -> {
        int expectedBucket = BucketIdentifier.getBucketId(delegate.getRecordKey(), "_row_key", numBuckets);
        assertEquals(expectedBucket, bucketIdFromFile,
            "record " + delegate.getRecordKey() + " was routed to the wrong bucket");
      });
    }

    // updates for existing keys must land in the same file groups established by the insert, proving the
    // bucket index resolves existing buckets without needing a separate index lookup structure
    String updateTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> updates = dataGen.generateUniqueUpdates(updateTime, 30);
    WriteClientTestUtils.startCommitWithTime(client, updateTime);
    List<WriteStatus> updateStatuses = client.upsert(updates, updateTime);
    assertNoErrors(updateStatuses);
    client.commit(updateTime, updateStatuses);

    Set<String> insertFileIds = insertStatuses.stream().map(WriteStatus::getFileId).collect(Collectors.toSet());
    for (WriteStatus status : updateStatuses) {
      assertTrue(insertFileIds.contains(status.getFileId()),
          "updates must reuse the existing bucket's file group, got a new fileId " + status.getFileId());
      // MoR bucket-index updates append to the existing file group's log file rather than rewriting the
      // base file
      assertTrue(FSUtils.isLogFile(new StoragePath(status.getStat().getPath()).getName()),
          "update must be written to a log file, got " + status.getStat().getPath());
    }

    // compaction must succeed and preserve the same file group ids
    Option<String> compactionTime = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTime.isPresent());
    HoodieWriteMetadata writeMetadata = client.compact(compactionTime.get());
    client.commitCompaction(compactionTime.get(), writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime.get()));
    client.close();
  }

  // Note on the "bucket count lowered on an existing table" scenario: a dedicated test for
  // JavaBucketIndexPartitioner#getPartition's bound check (see TestJavaBucketIndexPartitioner in
  // hudi-client-common's sibling package) directly constructs a record location whose fileId encodes an
  // out-of-range bucket id and asserts the exact failure message. An end-to-end integration reproduction
  // (write under a higher bucket count, then write again under a lower one) was attempted here but is not
  // reachable through any public write API: plain (non-prepped) insert/upsert always re-derives the bucket
  // id from HoodieSimpleBucketIndex#loadBucketIdToFileIdMappingForPartition using the *current* bucket
  // count, which by construction can never resolve to an out-of-range value; and prepped upserts
  // (JavaUpsertPreppedDeltaCommitActionExecutor#execute) bypass JavaBucketIndexPartitioner entirely,
  // grouping records directly by their pre-set fileId. The bound check is therefore pure defense-in-depth
  // against a future change to that lookup, exactly mirroring Spark's equivalent (unguarded) code, and is
  // fully covered by the unit test instead.

  /**
   * Proves the layout partitioner defaults to {@link JavaBucketIndexPartitioner} for a BUCKET index on the
   * Java engine without the caller setting hoodie.storage.layout.partitioner.class explicitly:
   * {@code HoodieLayoutConfig.Builder#setDefault} now threads engineType through (mirroring how
   * HoodieIndexConfig/HoodieMetadataConfig already pick engine-appropriate defaults), so a plain
   * {@code hoodie.index.type=BUCKET} config works out of the box on the Java engine.
   */
  @Test
  public void testBucketIndexDefaultsToJavaPartitionerWithoutExplicitLayoutConfig() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE,
        HoodieFailedWritesCleaningPolicy.EAGER)
        .withEngineType(EngineType.JAVA)
        // No .withLayoutConfig(...) at all: the partitioner class must be resolved purely from defaults.
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
            .withBucketNum("2")
            .withRecordKeyField("_row_key")
            .withIndexKeyField("_row_key")
            .build())
        .build();
    assertEquals(JavaBucketIndexPartitioner.class.getName(),
        config.getString(HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME),
        "the default layout partitioner for a BUCKET index on the Java engine must be JavaBucketIndexPartitioner");

    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 20);
    WriteClientTestUtils.startCommitWithTime(client, insertTime);
    List<WriteStatus> insertStatuses = client.insert(inserts, insertTime);
    assertNoErrors(insertStatuses);
    client.commit(insertTime, insertStatuses);
    assertFalse(insertStatuses.isEmpty());
    // fileId is bucket-prefixed (parses as a bucket id), proving JavaBucketIndexPartitioner actually
    // handled the write rather than a generic size-based partitioner
    for (WriteStatus status : insertStatuses) {
      BucketIdentifier.bucketIdFromFileId(status.getFileId());
    }

    String updateTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> updates = dataGen.generateUniqueUpdates(updateTime, 5);
    WriteClientTestUtils.startCommitWithTime(client, updateTime);
    List<WriteStatus> updateStatuses = client.upsert(updates, updateTime);
    assertNoErrors(updateStatuses);
    client.commit(updateTime, updateStatuses);
    client.close();
  }

  /**
   * The Java engine has no equivalent of Spark's {@code SparkPartitionBucketIndexPartitioner}: partition-level
   * bucket index (hoodie.bucket.index.partition.expressions) must be rejected with a clear message instead of
   * silently mis-routing records.
   */
  @Test
  public void testPartitionLevelBucketIndexRejectedOnJavaEngine() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE,
        HoodieFailedWritesCleaningPolicy.EAGER)
        .withEngineType(EngineType.JAVA)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
            .withBucketNum("2")
            .withRecordKeyField("_row_key")
            .withIndexKeyField("_row_key")
            .build())
        // "<regex>,<bucketNumber>": a valid, catch-all partition-level bucket expression (RegexRuleEngine
        // format), so tagging succeeds and the write actually reaches JavaBucketIndexPartitioner instead of
        // failing earlier for an unrelated (invalid expression syntax) reason.
        .withProps(Collections.singletonMap(HoodieIndexConfig.BUCKET_INDEX_PARTITION_EXPRESSIONS.key(), ".*,4"))
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 10);
    WriteClientTestUtils.startCommitWithTime(client, insertTime);
    Exception e = assertThrows(Exception.class, () -> client.insert(inserts, insertTime));
    // BaseWriteHelper#write wraps any Throwable in a HoodieUpsertException, and ReflectionUtils#loadClass
    // wraps the partitioner constructor's IllegalArgumentException in a HoodieException, so walk the full
    // cause chain rather than assuming a single level of wrapping.
    Throwable cause = e;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    assertTrue(cause.getMessage() != null && cause.getMessage().contains("not supported for the Java engine"),
        "unexpected exception: " + e);

    // bulk_insert must be rejected the same way: JavaBucketIndexBulkInsertPartitioner mirrors the same
    // partition-level-bucket-index rejection as JavaBucketIndexPartitioner, so a bulk_insert can't create a
    // table that the (non-bulk_insert) Java engine writers would then be unable to route records for.
    String bulkInsertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> bulkInserts = dataGen.generateInserts(bulkInsertTime, 10);
    WriteClientTestUtils.startCommitWithTime(client, bulkInsertTime);
    Exception bulkInsertEx = assertThrows(Exception.class, () -> client.bulkInsert(bulkInserts, bulkInsertTime));
    Throwable bulkInsertCause = bulkInsertEx;
    while (bulkInsertCause.getCause() != null) {
      bulkInsertCause = bulkInsertCause.getCause();
    }
    assertTrue(bulkInsertCause.getMessage() != null && bulkInsertCause.getMessage().contains("not supported for the Java engine"),
        "unexpected exception: " + bulkInsertEx);
    client.close();
  }

  /**
   * Bug-fix regression: {@code JavaBucketIndexBulkInsertPartitioner} must sort records within each
   * (partition path, bucket) group whenever {@code arePartitionRecordsSorted()} requires it (GLOBAL_SORT
   * bulk insert mode here), mirroring Spark's {@code RDDBucketIndexPartitioner#doPartitionAndSortByRecordKey}
   * -- previously it only grouped records by bucket and never sorted within a group.
   */
  @Test
  public void testBulkInsertSortsRecordsWithinBucketGroup() throws Exception {
    Properties props = getPropertiesForKeyGen(true);
    props.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withProps(Collections.singletonMap(HoodieTableConfig.ORDERING_FIELDS.key(), "ts"))
        .forTable("test")
        .withPath(basePath)
        .withSchema(jsonSchema)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadClass(OverwriteWithLatestAvroPayload.class.getName()).build())
        .withBulkInsertSortMode(BulkInsertSortMode.GLOBAL_SORT.name())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketNum("1")
            .build())
        .withPopulateMetaFields(true)
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    // a single bucket (withBucketNum("1")) so all three records land in the same (partition, bucket) group,
    // deliberately out of record-key order.
    List<String> dataset = Arrays.asList("id3,Chris,33,3,par1", "id1,Danny,11,1,par1", "id2,Betty,22,2,par1");
    String insertTime = WriteClientTestUtils.createNewInstantTime();
    List<WriteStatus> writeStatuses = writeData(client, insertTime, dataset, true, WriteOperationType.BULK_INSERT, true);
    assertFalse(writeStatuses.isEmpty());
    for (WriteStatus status : writeStatuses) {
      assertFalse(FSUtils.isLogFile(new StoragePath(status.getStat().getPath()).getName()));
    }

    File baseFile = new File(basePath);
    File[] partitionDirs = baseFile.listFiles(file -> !file.getName().startsWith("."));
    assertNotNull(partitionDirs);
    assertEquals(1, partitionDirs.length);
    File[] dataFiles = partitionDirs[0].listFiles(file -> FSUtils.isBaseFile(file.getName()));
    assertNotNull(dataFiles);
    assertEquals(1, dataFiles.length, "a single bucket must produce a single base file");
    StoragePath path = new StoragePath(dataFiles[0].getAbsolutePath());
    List<GenericRecord> records = HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(path).readAvroRecords(storage, path);
    List<String> recordKeysInFileOrder = records.stream()
        .map(record -> getFieldValue(record, "_hoodie_record_key"))
        .collect(Collectors.toList());

    // written in ascending record-key order, not the (id3, id1, id2) insertion order.
    assertEquals(Arrays.asList("id1", "id2", "id3"), recordKeysInFileOrder,
        "records within a bucket group must be sorted by record key under GLOBAL_SORT bulk insert mode");
    client.close();
  }

  private HoodieWriteConfig.Builder baseNbccConfigBuilder(HoodieTableType tableType) {
    return HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .forTable("nbcc-validation")
        .withProps(Collections.singletonMap(TYPE.key(), tableType.name()))
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
            .withBucketNum("1")
            .withRecordKeyField("_row_key")
            .withIndexKeyField("_row_key")
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build());
  }

  private HoodieWriteConfig createHoodieWriteConfig() {
    return createHoodieWriteConfig(false);
  }

  private HoodieWriteConfig createHoodieWriteConfig(boolean fullUpdate) {
    String payloadClassName = PartialUpdateAvroPayload.class.getName();
    if (fullUpdate) {
      payloadClassName = OverwriteWithLatestAvroPayload.class.getName();
    }
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
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadClass(payloadClassName).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(1024).build())
        // No explicit .withLayoutConfig(...): relies on HoodieLayoutConfig defaulting the layout partitioner
        // to JavaBucketIndexPartitioner for a BUCKET index on the JAVA engine (see engineType wiring in
        // HoodieLayoutConfig.Builder#setDefault / HoodieWriteConfig.Builder#setDefaults).
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketNum("1")
            .build())
        .withPopulateMetaFields(true)
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withEmbeddedTimelineServerEnabled(false)
        // Explicit, rather than relying on HoodieLockConfig's infer-from-index-type default: the nested
        // HoodieLockConfig.Builder here builds against its own (bucket-index-unaware) HoodieWriteConfig
        // snapshot, so the strategy-inference logic that would normally pick
        // BucketIndexConcurrentFileWritesConflictResolutionStrategy for a bucket-index table never sees the
        // BUCKET index type and would otherwise fall back to the non-bucket-aware default.
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new BucketIndexConcurrentFileWritesConflictResolutionStrategy())
            .build())
        .build();
  }

  private void checkWrittenData(Map<String, String> expected, int partitions) throws IOException {
    File baseFile = new File(basePath);
    FileFilter filter = file -> !file.getName().startsWith(".");
    File[] partitionDirs = baseFile.listFiles(filter);
    assertNotNull(partitionDirs);
    assertEquals(partitions, partitionDirs.length);
    for (File partitionDir : partitionDirs) {
      File[] dataFiles = partitionDir.listFiles(file -> FSUtils.isBaseFile(file.getName()));
      assertNotNull(dataFiles);
      assertTrue(dataFiles.length > 0);
      File latestDataFile = Arrays.stream(dataFiles)
          .max(Comparator.comparing(f -> FSUtils.getCommitTime(f.getName())))
          .orElse(dataFiles[0]);
      StoragePath path = new StoragePath(latestDataFile.getAbsolutePath());
      List<GenericRecord> records = HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(path).readAvroRecords(storage, path);
      List<String> readBuffer = records.stream().map(TestJavaNonBlockingConcurrencyControl::filterOutVariables).collect(Collectors.toList());
      Collections.sort(readBuffer);
      assertEquals(expected.get(partitionDir.getName()), readBuffer.toString());
    }
  }

  private static String filterOutVariables(GenericRecord genericRecord) {
    List<String> fields = new ArrayList<>();
    fields.add(getFieldValue(genericRecord, "_hoodie_record_key"));
    fields.add(getFieldValue(genericRecord, "_hoodie_partition_path"));
    fields.add(getFieldValue(genericRecord, "id"));
    fields.add(getFieldValue(genericRecord, "name"));
    fields.add(getFieldValue(genericRecord, "age"));
    fields.add(genericRecord.get("ts").toString());
    fields.add(genericRecord.get("part").toString());
    return String.join(",", fields);
  }

  private static String getFieldValue(GenericRecord genericRecord, String fieldName) {
    return genericRecord.get(fieldName) != null ? genericRecord.get(fieldName).toString() : null;
  }

  private boolean baseDataFileExists() {
    List<File> dirsToCheck = new ArrayList<>();
    dirsToCheck.add(new File(basePath));
    while (!dirsToCheck.isEmpty()) {
      File dir = dirsToCheck.remove(0);
      for (File file : Objects.requireNonNull(dir.listFiles())) {
        if (!file.getName().startsWith(".")) {
          if (file.isDirectory()) {
            dirsToCheck.add(file);
          } else if (FSUtils.isBaseFile(file.getName())) {
            return true;
          }
        }
      }
    }
    return false;
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

  private List<HoodieRecord> str2HoodieRecord(List<String> records, boolean fullUpdate) {
    return records.stream().map(recordStr -> {
      GenericRecord record = str2GenericRecord(recordStr);
      OverwriteWithLatestAvroPayload payload;
      if (fullUpdate) {
        payload = new OverwriteWithLatestAvroPayload(record, (Long) record.get("ts"));
      } else {
        payload = new PartialUpdateAvroPayload(record, (Long) record.get("ts"));
      }
      return new HoodieAvroRecord<>(new HoodieKey((String) record.get("id"), (String) record.get("part")), payload);
    }).collect(Collectors.toList());
  }

  private List<WriteStatus> writeData(HoodieJavaWriteClient client, String instant, List<String> records,
                                      boolean doCommit, WriteOperationType operationType) throws IOException {
    return writeData(client, instant, records, doCommit, operationType, false);
  }

  private List<WriteStatus> writeData(HoodieJavaWriteClient client, String instant, List<String> records,
                                      boolean doCommit, WriteOperationType operationType, boolean fullUpdate) throws IOException {
    List<HoodieRecord> recordList = str2HoodieRecord(records, fullUpdate);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> writeStatuses;
    switch (operationType) {
      case INSERT:
        writeStatuses = client.insert(recordList, instant);
        break;
      case UPSERT:
        writeStatuses = client.upsert(recordList, instant);
        break;
      case BULK_INSERT:
        writeStatuses = client.bulkInsert(recordList, instant);
        break;
      default:
        throw new UnsupportedOperationException(operationType + " is not supported yet in this test!");
    }
    assertNoErrors(writeStatuses);
    if (doCommit) {
      List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      boolean committed = client.commitStats(instant, writeStats, Option.empty(), metaClient.getCommitActionType());
      assertTrue(committed);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }

  private static void assertNoErrors(List<WriteStatus> statuses) {
    for (WriteStatus status : statuses) {
      assertFalse(status.hasErrors(), "Errors found in write of partition " + status.getPartitionPath());
    }
  }

  // BucketIndexConcurrentFileWritesConflictResolutionStrategy#hasConflict only logs the intersecting bucket
  // ids (log.info "Found conflicting writes between ... intersecting bucket ids ..."); the actual thrown
  // HoodieWriteConflictException message is built by the parent
  // SimpleConcurrentFileWritesConflictResolutionStrategy from the two operations' type/instant/state, e.g.
  // "Cannot resolve conflicts for overlapping writes. bulk_insert operation (instant: ..., state: INFLIGHT)
  // has overlapping file groups with insert operation (instant: ..., state: COMPLETED)." Assert on that
  // (real, verified) shape -- naming both conflicting operation types -- so these tests can't stay green
  // for an unrelated HoodieWriteConflictException.
  private static void assertConflictMessageIdentifiesOperations(HoodieWriteConflictException conflict,
                                                                 WriteOperationType thisOp, WriteOperationType otherOp) {
    String message = conflict.getMessage();
    assertTrue(message != null && message.contains("Cannot resolve conflicts for overlapping writes")
            && message.contains("overlapping file groups"),
        "expected the conflict message to describe overlapping file groups, got: " + message);
    assertTrue(message.contains(thisOp.value() + " operation") && message.contains(otherOp.value() + " operation"),
        "expected the conflict message to name both conflicting operations (" + thisOp.value() + ", " + otherOp.value()
            + "), got: " + message);
  }
}
