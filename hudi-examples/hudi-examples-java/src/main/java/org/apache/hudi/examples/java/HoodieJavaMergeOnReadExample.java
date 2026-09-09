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

package org.apache.hudi.examples.java;

import org.apache.hudi.client.HoodieJavaReadClient;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/** Standalone MOR writes, snapshot reads and compaction using native Avro records and event-time ordering. */
public class HoodieJavaMergeOnReadExample {
  private static final HoodieSchema SCHEMA = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"example_record\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"partition\",\"type\":\"string\"},"
      + "{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"value\",\"type\":\"string\"}]}");

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: HoodieJavaMergeOnReadExample <new-table-path>");
    }
    String tablePath = args[0];
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConf(new Configuration());
    if (HadoopFSUtils.getFs(tablePath, storageConf).exists(new Path(tablePath))) {
      throw new IllegalArgumentException("Use a new table path for this example: " + tablePath);
    }
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    props.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition");

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withEngineType(EngineType.JAVA)
        .withPath(tablePath).forTable("java_mor_example").withSchema(SCHEMA.toString()).withProps(props)
        .withRecordMergeMode(RecordMergeMode.EVENT_TIME_ORDERING)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withEmbeddedTimelineServerEnabled(false).build();

    HoodieTableMetaClient.newTableBuilder().fromProperties(config.getProps())
        .setTableType(HoodieTableType.MERGE_ON_READ).setTableName("java_mor_example")
        .setRecordMergeMode(RecordMergeMode.EVENT_TIME_ORDERING).setPayloadClassName(null)
        .initTable(storageConf, tablePath);

    HoodieJavaEngineContext context = new HoodieJavaEngineContext(storageConf);
    try (HoodieJavaWriteClient<IndexedRecord> writer = new HoodieJavaWriteClient<>(context, config);
         HoodieJavaReadClient reader = new HoodieJavaReadClient(context, tablePath, new TypedProperties())) {
      String instant = writer.startCommit();
      commit(writer, instant, writer.insert(Arrays.asList(record("a", 10, "initial"), record("b", 10, "delete-me")), instant));

      instant = writer.startCommit();
      commit(writer, instant, writer.upsert(Collections.singletonList(record("a", 30, "latest-event")), instant));
      instant = writer.startCommit();
      commit(writer, instant, writer.upsert(Collections.singletonList(record("a", 20, "late-arrival")), instant));

      instant = writer.startCommit();
      commit(writer, instant, writer.delete(Collections.singletonList(new HoodieKey("b", "example")), instant));

      System.out.println("Snapshot: a retains latest-event; b is deleted");
      printRows(reader.readSnapshot());

      Option<String> compaction = writer.scheduleCompaction(Option.empty());
      if (compaction.isPresent()) {
        HoodieWriteMetadata<List<WriteStatus>> metadata = writer.compact(compaction.get());
        checkStatuses(metadata.getWriteStatuses());
        writer.commitCompaction(compaction.get(), metadata, Option.empty());
      }
      System.out.println("Read-optimized after compaction");
      printRows(reader.readOptimized());
    }
  }

  private static HoodieRecord<IndexedRecord> record(String key, long timestamp, String value) {
    GenericData.Record data = new GenericData.Record(SCHEMA.toAvroSchema());
    data.put("id", key);
    data.put("partition", "example");
    data.put("ts", timestamp);
    data.put("value", value);
    return new HoodieAvroIndexedRecord(new HoodieKey(key, "example"), data, timestamp);
  }

  private static void commit(HoodieJavaWriteClient<IndexedRecord> client, String instant, List<WriteStatus> statuses) {
    checkStatuses(statuses);
    if (!client.commit(instant, statuses)) {
      throw new IllegalStateException("Commit did not complete: " + instant);
    }
  }

  private static void checkStatuses(List<WriteStatus> statuses) {
    for (WriteStatus status : statuses) {
      if (status.hasErrors()) {
        throw new IllegalStateException("Write failed: " + status.getErrors(), status.getGlobalError());
      }
    }
  }

  private static void printRows(ClosableIterator<HoodieRecord<IndexedRecord>> rows) {
    try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator = rows) {
      while (iterator.hasNext()) {
        System.out.println(iterator.next().getData());
      }
    }
  }
}
