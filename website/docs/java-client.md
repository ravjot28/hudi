---
title: Standalone Java Client
description: Read and write Hudi tables from a Java application without a Spark or Flink runtime.
keywords: [hudi, java, merge on read, snapshot, incremental]
---

<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

The `hudi-java-client` module lets an application use Hudi without starting a Spark or Flink runtime.
It provides `HoodieJavaWriteClient`, `HoodieJavaTableServiceClient`, and `HoodieJavaReadClient`.
Use this approach when the workload fits the application's resources and does not require a distributed compute engine.

The reader described here is part of the unreleased development version. It is not available in earlier released artifacts.
Use matching Hudi versions for the client and its dependencies.

## Read an existing table

This example prints the current snapshot of an existing Copy-on-Write or Merge-on-Read table.
It uses the storage configuration to access the table, so configure filesystem credentials and connectors as you would for the writer.

```java
import org.apache.hudi.client.HoodieJavaReadClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;

public class JavaSnapshotExample {
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Pass an existing Hudi table base path");
    }
    HoodieJavaEngineContext context = new HoodieJavaEngineContext(
        HadoopFSUtils.getStorageConf(new Configuration()));
    try (HoodieJavaReadClient client =
             new HoodieJavaReadClient(context, args[0], new TypedProperties());
         ClosableIterator<HoodieRecord<IndexedRecord>> rows = client.readSnapshot()) {
      while (rows.hasNext()) {
        System.out.println(rows.next().getData());
      }
    }
  }
}
```

Close each returned iterator, including when you stop reading early. Closing the client does not close an iterator already returned to the caller.
Each call loads a fresh timeline view. An iterator continues to use its own view while later calls can observe newer completed writes.
Table retention and cleaning still determine which historical files remain available.

| Method | Result |
| --- | --- |
| `readSnapshot()` | Latest merged records; for MOR, merges base files and completed log updates. |
| `readSnapshot(requestedInstant)` | Snapshot at a completed instant's requested time on the active timeline. |
| `readOptimized()` | Latest completed base files only. MOR updates in logs become visible after compaction; a log-only file group is absent. |
| `readIncremental(startCompletionTime, endCompletionTime)` | Merged records selected using completion-time bounds, inclusive at both ends. Deleted records are filtered out. |

Time travel uses **requested time**; incremental bounds use **completion time**. These timestamps are not interchangeable.
A null incremental start selects the latest completed instant. The start value `"earliest"` with a null end returns the current snapshot, not every historical version or a change-event stream.
A non-earliest start with a null end reads through the latest completed instant. `IncrementalConfig` allows callers to skip compaction, clustering, or insert-overwrite instants.

## Write and maintain MOR tables

Initialize the table as `MERGE_ON_READ` and use `HoodieJavaWriteClient` for inserts, upserts, deletes, and prepared-record operations.
Set `EngineType.JAVA` on `HoodieWriteConfig.Builder`. Check returned write statuses and follow the configured commit policy.
The existing [Java write example](https://github.com/apache/hudi/blob/master/hudi-examples/hudi-examples-java/src/main/java/org/apache/hudi/examples/java/HoodieJavaWriteClientExample.java) shows table initialization and ordinary write operations.

Regular compaction and log compaction are separate table services. Scheduling produces a plan only when its eligibility criteria are met; execute and commit a scheduled plan through the Java table-service/write APIs.
See [compaction](compaction.md), [clustering](clustering.md), [cleaning](cleaning.md), and [rollback](rollbacks.md) for the underlying table semantics.
The Java table API also supports partition/table replacement, partition deletion, partition TTL, and bootstrap; some operations are exposed on the table rather than as write-client convenience methods.

### Simple bucket index

For a Java bucket-index table, set `hoodie.index.type=BUCKET`, `hoodie.index.bucket.engine=SIMPLE`, and `hoodie.bucket.index.num.buckets` when creating the table.
Use the same record-key and index-key fields as the write configuration. With `EngineType.JAVA`, layout defaults select the Java bucket partitioner.
Bulk insert routes records by partition and bucket and preserves existing bucket file IDs.

Choose the bucket count before creating the table. Changing it on an existing Java bucket table is unsupported; rebuilding the table is required.
Partition-specific bucket expressions and the consistent-hashing bucket engine are unsupported in the Java client.
For multiple writers, configure an appropriate concurrency mode and lock provider as described in [concurrency control](concurrency_control.md); an in-process lock does not coordinate separate application processes.

## Scope and resource limits

Only one file group's merge reader is open at a time, and its merge buffer can spill to disk. Planning memory still grows with the number of selected partitions and file slices.
This is not a constant-memory or distributed query API. Size application heap and spill storage for the workload.

The reader supports internal-schema reconciliation, including renamed fields, using the shared file-group reader.
The following boundaries remain:

- Bounded incremental ranges reaching the archived timeline are rejected. Read a current snapshot or choose an active-timeline range.
- Archived time-travel targets are unsupported; active targets also depend on retained data files.
- Incremental filtering requires `hoodie.populate.meta.fields=true`.
- LSM tree storage is rejected because it needs a different reader.
- The API returns merged records, not CDC events. CDC-enabled Java writes are discoverable through the shared CDC extractor, but this client has no CDC-output method.
- Java bootstrap reads external files directly and does not support custom full-bootstrap input providers.

### Payload transport compatibility

Schema-aware Avro payload decoding retains the writer schema for projections and supported schema evolution.
Spark's registered Kryo transport retains its previous compact representation. Default non-Spark Kryo serialization includes the writer schema and requires an updated reader; older readers cannot decode that representation.
Transports that supply the writer schema separately can select `BaseAvroPayload.useLegacyKryoFormat(kryo)`. That mode does not provide self-contained schema retention.
Do not mix old and new default Kryo readers/writers without an explicit compatibility plan.
