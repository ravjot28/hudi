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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
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
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for a pre-existing upstream data-corruption bug: any merge against
 * already-persisted data (CoW small-file/update rewrites, MoR log-over-base-file merges) silently
 * nulled out data fields (or, for {@link PartialUpdateAvroPayload}, silently dropped updates) whenever
 * the user-supplied write schema declared the five {@code _hoodie_} meta fields explicitly.
 *
 * <p>Root cause: {@link org.apache.hudi.common.model.BaseAvroPayload#getRecord} re-encoded a payload
 * with its true (writer) schema but decoded with the caller-requested schema used as both writer and
 * reader, causing Avro to resolve fields positionally instead of by name whenever the two schemas
 * differed structurally (e.g. a meta-fields-declaring schema vs. a merge path's data-only projection of
 * it). A second, independent defect compounded this on MoR: {@code RunCompactionActionExecutor} failed
 * to unconditionally copy the write config before compaction, so {@code HoodieCompactor}'s
 * {@code config.setSchema(...)} (a data-only, meta-fields-stripped schema) permanently mutated the live
 * write client's config, corrupting every subsequent log append to that file group even outside the
 * original schema-declaring-meta-fields case.
 *
 * <p>Unlike {@link TestHoodieJavaReadClient}, this suite deliberately DOES declare the five
 * {@code _hoodie_*} meta fields in its write schema (mirroring what a real user schema often looks
 * like), since that is precisely the trigger for the bug being regression-tested here.
 */
public class TestAvroPayloadSchemaMismatch extends HoodieJavaClientTestHarness {

  private final String jsonSchemaWithMetaFields = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"testRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_commit_seqno\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_record_key\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_partition_path\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"_hoodie_file_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"age\", \"type\": [\"null\", \"int\"]},\n"
      + "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"part\", \"type\": [\"null\", \"string\"]}\n"
      + "  ]\n"
      + "}";

  private final HoodieSchema schema = HoodieSchema.parse(jsonSchemaWithMetaFields);

  @Override
  protected void initMetaClient() {
    // metaClient is (re)initialized per test, once the test-specific HoodieWriteConfig is built.
  }

  // -------------------------------------------------------------------------
  //  1. CoW small-file merge with a meta-fields-declaring schema
  // -------------------------------------------------------------------------

  @Test
  public void testCowSmallFileMergeWithMetaDeclaringSchemaPreservesDataFields() throws Exception {
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
        .withSchema(jsonSchemaWithMetaFields)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadClass(OverwriteWithLatestAvroPayload.class.getName()).build())
        // default (nonzero) small-file size: the second insert into the same partition must trigger a
        // small-file merge (rewrite) against the first commit's base file, which is the CoW reach of the bug.
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(HoodieIndex.IndexType.SIMPLE).build())
        .withPopulateMetaFields(true)
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1");
    // id2 lands in the same partition (par1): with default small-file settings this rewrites/merges
    // against id1's existing base file rather than writing a brand-new one.
    writeAndCommit(client, "id2,Betty,2,2,par1");

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,Danny,1,1,par1");
      expected.put("id2", "id2,Betty,2,2,par1");
      assertEquals(expected, snapshot,
          "CoW small-file merge must preserve data fields (not null them) when the write schema declares meta fields");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  2. MoR bucket-index: upsert -> compact -> new key logged over base, meta-declaring schema
  // -------------------------------------------------------------------------

  @Test
  public void testMorBucketIndexLogOverCompactedBaseWithMetaDeclaringSchemaPreservesDataFields() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    String schemaBeforeCompaction = client.getConfig().getSchema();

    writeAndCommit(client, "id1,Danny,1,1,par1");
    writeAndCommit(client, "id1,Danny,11,2,par1", WriteOperationType.UPSERT);
    String compactionInstant = (String) client.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client.compact(compactionInstant);
    client.commitCompaction(compactionInstant, writeMetadata, Option.empty());
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Defect B, checked directly: compacting must never mutate the live client's write schema.
    assertEquals(schemaBeforeCompaction, client.getConfig().getSchema(),
        "compaction must not permanently mutate the write client's configured schema");

    // a new key into the SAME bucket (numBuckets=1): logged over the just-compacted base file, the MoR
    // reach of the bug (via the post-compaction schema mutation chain, when defect B isn't fixed).
    writeAndCommit(client, "id2,Betty,2,3,par1");

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      Map<String, String> expected = new HashMap<>();
      expected.put("id1", "id1,Danny,11,2,par1");
      expected.put("id2", "id2,Betty,2,3,par1");
      assertEquals(expected, snapshot,
          "a log entry written over a compacted base file must preserve data fields when the write schema declares meta fields");
    }
    client.close();
  }

  // -------------------------------------------------------------------------
  //  4. Finding 2: compaction commit metadata must record the RESOLVED TABLE schema, not a
  //     drifted/partial client-configured schema (e.g. a MergeInto-style client)
  // -------------------------------------------------------------------------

  @Test
  public void testCompactionCommitMetadataRecordsResolvedTableSchemaNotClientSchema() throws Exception {
    HoodieWriteConfig config = nbccBucketConfig();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1");
    writeAndCommit(client, "id1,Danny,11,2,par1", WriteOperationType.UPSERT);

    // Simulate a MergeInto-style client whose configured write schema has drifted from the table's actual
    // persisted schema (e.g. a partial schema naming only the columns touched by a MERGE INTO ... clause).
    // This must never end up recorded as the compaction commit's schema: HoodieCompactor resolves and sets
    // the real table schema onto its own config copy regardless, and the commit metadata must reflect that
    // resolved copy, not whatever the live client's config says.
    String partialSchema = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"partial\", \"namespace\":\"org.apache.hudi\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
        + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"]}\n"
        + "  ]\n"
        + "}";
    client.getConfig().setSchema(partialSchema);

    String compactionInstant = (String) client.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata writeMetadata = client.compact(compactionInstant);
    client.commitCompaction(compactionInstant, writeMetadata, Option.empty());
    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieInstant instant = metaClient.getActiveTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(i -> i.requestedTime().equals(compactionInstant))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("compaction instant not found on active timeline"));
    HoodieCommitMetadata commitMetadata = metaClient.getActiveTimeline().readCommitMetadata(instant);
    String recordedSchema = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);

    assertFalse(recordedSchema.contains("\"partial\""),
        "compaction commit metadata must not record the stale/partial client-configured schema");
    // HoodieCompactor resolves the reader schema via TableSchemaResolver#getTableSchema(false) (meta
    // fields excluded), so the recorded schema is data-only -- assert it has the data fields the partial
    // client schema deliberately omitted ("age", "ts", "part"), proving it's the real resolved table
    // schema and not the 2-field (id, name only) drifted client schema.
    assertTrue(recordedSchema.contains("\"age\"") && recordedSchema.contains("\"ts\"") && recordedSchema.contains("\"part\""),
        "compaction commit metadata must record the actual resolved table schema (all data fields), not the client's drifted schema");
    client.close();
  }

  // -------------------------------------------------------------------------
  //  3. PartialUpdateAvroPayload: update must be applied, not silently dropped
  // -------------------------------------------------------------------------

  @Test
  public void testPartialUpdateAvroPayloadAppliesUpdateWithMetaDeclaringSchema() throws Exception {
    Properties props = new Properties();
    props.put(TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "part");
    props.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id");
    props.put(HoodieTableConfig.PARTITION_FIELDS.key(), "part");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withProps(Collections.singletonMap(HoodieTableConfig.ORDERING_FIELDS.key(), "ts"))
        .forTable("test")
        .withPath(basePath)
        .withSchema(jsonSchemaWithMetaFields)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadClass(PartialUpdateAvroPayload.class.getName()).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(HoodieIndex.IndexType.SIMPLE).build())
        .withPopulateMetaFields(true)
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, config.getProps());
    HoodieJavaWriteClient client = getHoodieWriteClient(config, false);

    writeAndCommit(client, "id1,Danny,1,1,par1", WriteOperationType.INSERT, true);
    // an update with a higher ordering value (ts=2): the merge must actually apply it, not silently drop
    // it (the PartialUpdateAvroPayload symptom of this bug, distinct from the OverwriteWithLatestAvroPayload
    // null-out symptom asserted by the other two tests).
    writeAndCommit(client, "id1,Danny,111,2,par1", WriteOperationType.UPSERT, true);

    try (HoodieJavaReadClient readClient = newReadClient()) {
      Map<String, String> snapshot = readAll(readClient.readSnapshot());
      assertEquals(Collections.singletonMap("id1", "id1,Danny,111,2,par1"), snapshot,
          "PartialUpdateAvroPayload's update must be applied, not silently dropped, when the write schema declares meta fields");
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
    return HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withProps(Collections.singletonMap(HoodieTableConfig.ORDERING_FIELDS.key(), "ts"))
        .forTable("test")
        .withPath(basePath)
        .withSchema(jsonSchemaWithMetaFields)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadClass(OverwriteWithLatestAvroPayload.class.getName()).build())
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

  private void writeAndCommit(HoodieJavaWriteClient client, String record) throws IOException {
    writeAndCommit(client, record, WriteOperationType.INSERT);
  }

  private void writeAndCommit(HoodieJavaWriteClient client, String record, WriteOperationType opType) throws IOException {
    writeAndCommit(client, record, opType, false);
  }

  private void writeAndCommit(HoodieJavaWriteClient client, String record, WriteOperationType opType, boolean partialUpdatePayload) throws IOException {
    String instant = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> records = Collections.singletonList(str2HoodieRecord(record, partialUpdatePayload));
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
    List<HoodieWriteStat> writeStats = statuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
    boolean committed = client.commitStats(instant, writeStats, Option.empty(), metaClient.getCommitActionType());
    assertTrue(committed);
    metaClient = HoodieTableMetaClient.reload(metaClient);
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

  private HoodieRecord str2HoodieRecord(String str, boolean partialUpdatePayload) {
    GenericRecord record = str2GenericRecord(str);
    Long ts = (Long) record.get("ts");
    Object payload = partialUpdatePayload ? new PartialUpdateAvroPayload(record, ts) : new OverwriteWithLatestAvroPayload(record, ts);
    return new HoodieAvroRecord<>(new HoodieKey((String) record.get("id"), (String) record.get("part")), (HoodieRecordPayload) payload);
  }

  /**
   * Reads every record into a map keyed by record key, valued by a comparable summary of its fields
   * ("id,name,age,ts,part"), and closes the iterator.
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

  private static String summarize(GenericRecord record) {
    return String.join(",",
        String.valueOf(record.get("id")),
        String.valueOf(record.get("name")),
        String.valueOf(record.get("age")),
        String.valueOf(record.get("ts")),
        String.valueOf(record.get("part")));
  }
}
