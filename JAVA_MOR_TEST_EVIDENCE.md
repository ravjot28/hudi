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

# Java standalone MoR test evidence

## Native RecordMerger implementation validation — 2026-09-08

Current incremental implementation is on local `pr/java-mor-record-merger`
(`a0f0f9308cf755ef4dc07571003db20b5fd6ba3b`), based on prepared read/write branch
`29ee4d8214baee39a05a4478bf7eea96a751540f`. It adds 47 native merger cases, a runnable
native MOR example, caller-list protection at the public delete boundary and shared spill-buffer
cleanup when initial log scanning fails. The internal JavaDeleteHelper and its existing test
remain unchanged. An intermediate helper change failed its identity contract and was removed.

**The final full Java run is not strictly green:** 526 cases, zero final failures/errors/skips,
but one retry-recovered `TestJavaHoodieBackedMetadata.testReattemptOfFailedClusteringCommit`
failure. The strict no-flake gate correctly failed. This is the same metadata file-list failure
previously reproduced on the complete pre-MOR baseline; earlier baseline evidence and the deferred
HFile investigation remain below. No test was disabled and no assertion or gate was weakened.
All 47 new native cases and all 220 reader cases passed without retries.

Additional validation: exact final native+unchanged helper selection passed 48 cases with the
original Apache BaseAvroPayload in a private checkout/cache, and 48 with the current payload fix.
Selected shared payload/loader/merger tests passed 19. Java, common and example Checkstyle passed.
The new example compiled and ran on Java 11, producing the expected surviving updated record in
snapshot and read-optimized output. The final website build and 26 generated links/anchors passed.
These local checks do not constitute new remote CI or a complete Spark/Flink run.

Known shared partial-update limitation is preserved separately on local
`pr/java-mor-partial-update-regression` (`09a55b0890fc2d1791769b390eefab7ea53b8bcd`):
IGNORE_DEFAULTS event-time merging can select a different field value after moving a row into a
base file. A pure shared-merger test reproduces it without the Java writer. The branch retains
that failing assertion and four Java regression parameters; it is not merge-ready. This prevents
claiming all-edge-case parity or production readiness. The existing legacy payload PR #19872
remains separate; no further payload serialization change was made in this continuation.

Website review branch: `pr/java-mor-record-merger-docs` at
`021747959a011bb4f1ad9fcb3984dbe9cb614677`. New review branches have not been pushed.
The existing reply to Vinoth was edited with the plan:
https://github.com/apache/hudi/discussions/19798#discussioncomment-18359759

Evidence: `target/java-mor-merger-assessment/IMPLEMENTATION.md`, `final-java-results.json`,
`final-java-reports/`, `final-source-hashes.json`, `native-branches.json`, `NATIVE_PR_BODY.md`,
and logs `combined-java-final.log`, `exact-native-original-tests.log`,
`final-native-and-delete-helper.log`, `final-shared-tests.log`, `shared-partial-grouping-reproducer.log`,
`final-java-boundary-style.log`, `final-common-style.log`, `final-example-style.log`,
`example-0.log`, `example-1.log`, `website-native-final-build.log`.

Next performance phase: user reports Spark-on-AKS degradation with higher organization count at
200,000 messages/minute and wants a standalone Java comparison. A maximum of 32 partitions was
reported; partition type is pending clarification. User will arrange Azure resources/access after
we prepare the setup. No cloud resources were provisioned and no load was started. Source-based
fixture findings, measurement controls and pending consumer configuration are recorded in
`target/java-mor-performance-prep/README.md`. Java cost/speed benefits remain hypotheses to measure.

Older sections below are historical and may describe superseded implementation phases or totals.


Last updated: 2026-09-07

The tables and dated runs below preserve the test-only baseline: `RED` records a failure at that
stage, not the status after implementation. The user subsequently authorized production fixes.
Current results are recorded in the latest dated continuation below; it supersedes historical failure counts. Acceptance tests remain enabled.

Hudi's Surefire configuration retries failing tests up to three times (`rerunFailingTestsCount=3`).
Passing tests are not automatically repeated four times. Counts below refer to distinct test invocations,
including parameter rows, rather than retries.

## Passing invariants

| Test | Parity source or risk | What it proves | Result |
| --- | --- | --- | --- |
| `TestHoodieJavaReadClient#testSnapshotAndCompactionRespectEventTimeOrdering` | Flink event-time MoR read/compaction tests | A later commit with an older event time does not replace the logical winner, before or after compaction. | PASS |
| `TestHoodieJavaReadClient#testReadsIgnoreInflightCompactionBaseFileButSeeLaterDeltaCommit` | Spark read-optimized pending-compaction regression | Read-optimized ignores an uncommitted compaction base; snapshot still sees a later completed delta commit. | PASS |
| `TestHoodieJavaReadClient#testSnapshotSpillsMergeBufferAndCleansItOnClose` | Spark spill coverage | A one-byte merge budget forces disk spill, all 100 updated rows are returned, and iterator close removes spill state. | PASS |
| `TestHoodieJavaReadClient#testCdcEnabledMorWritesAreConsumableByCommonCdcExtractor` | Flink direct CDC-enabled MoR writes, mapped to the common boundary | Insert and update delta commits produce `LOG_FILE` CDC inference splits with source log paths. | PASS |
| `TestLazyConcatenatingIterator#testCloseAfterPartialIterationDoesNotOpenRemainingIterators` | Reader resource/performance risk | Construction opens no reader; partial iteration opens one; early close closes it without opening the rest. | PASS |
| `TestHoodieJavaClientOnMergeOnReadStorage#testCompactionAndLogCompactionSchedulingOrder` | Spark Java-client-level table-service ordering | Pending regular compaction blocks overlapping log-compaction planning; pending log compaction does not block regular compaction planning. | PASS (2 parameters) |
| `TestHoodieJavaClientOnMergeOnReadStorage#testInlineCompactionScheduling` | Spark inline scheduling test | `scheduleInlineCompaction` controls whether reaching the delta threshold creates a pending plan. | PASS (2 parameters) |
| `TestHoodieJavaClientOnMergeOnReadStorage#testCleanerPreservesRequestedCompactionFileSlice` | Spark cleaner with requested compaction | Cleaning actually runs, preserves the requested compaction boundary, and retains all 100 records. | PASS |
| strengthened `testWriteCommitCallbackFiresOnClustering` | Spark clustering-on-MoR data validation | Clustering completes/callbacks and preserves all 200 records, rather than proving only timeline state. | PASS |
| `TestJavaNonBlockingConcurrencyControl#testNonBlockingConcurrencyControlWithConcurrentDeleteAndUpsert` | Missing delete dimension in the Spark/Flink-derived NBCC matrix | Overlapping delete/upsert writes can complete out of start order, remain log-only, compact, and preserve the tombstone/update result. | PASS |
| `TestJavaNonBlockingConcurrencyControl#testNbccRecoversAbandonedInflightCompaction` | Spark failed-writer compaction recovery | A second writer completes one rollback, re-executes the original instant, leaves no pending/inflight rollback, and preserves data. | PASS |

The user-authored test classes already contain broader coverage for snapshot/read-optimized/incremental
reads, active time travel, empty tables, deletes, partition pruning, post-compaction writes, NBCC partial
updates/inflight instants, multi-base-file behavior, and six bulk-insert/insert concurrency orders. The
table above lists the new or materially strengthened cases from the hardening phase rather than
duplicating that whole inventory.

## Deliberate failing invariants

| Test | Observed failure | Meaning | Result |
| --- | --- | --- | --- |
| `TestHoodieJavaClientOnMergeOnReadStorage#testInsertPreppedUsesMorDeltaWritePath` | Write statuses point to base files. | `insertPreppedRecords` is routed through inherited CoW behavior instead of a MoR delta-commit executor. | RED |
| `TestHoodieJavaClientOnMergeOnReadStorage#testLogCompactionOnMORTable(false)` | `UnsupportedOperationException: Log compaction is not supported for this execution engine.` | Java cannot execute log compaction for log-only file groups. | RED |
| `TestHoodieJavaClientOnMergeOnReadStorage#testLogCompactionOnMORTable(true)` | Same exception after a normal compaction creates a base file. | Java cannot execute log compaction for base-plus-log file groups either; planning is not the problem. | RED |
| `TestHoodieJavaReadClient#testTimeTravelRejectsScheduledButIncompleteCompactionInstant` | Expected `IllegalArgumentException`, but no exception was thrown. | The API accepts a target that exists on the timeline but is not completed. | RED |
| `TestHoodieJavaReadClient#testSnapshotReconcilesInternalSchemaRenameInOldLogBlock` | Expected `full_name=Danny`, observed `null`. | Latest-schema-only reading does not reconcile the old log-block schema by InternalSchema field id. | RED |
| `TestHoodieJavaReadClient#testUnsupportedReadConfigurationsAreRejectedAtEntryPoint` | Neither no-meta incremental nor LSM snapshot throws. | Documented/structural exclusions are accepted and can fail later or return incorrect results instead of failing fast. | RED (2 assertions) |
| `TestBaseAvroPayload#getRecordHandlesProjectionSerializationAndEvolutionWithoutDataLoss` | Four failures: malformed data after Kryo, alias rejection, nested rename becomes null, added default causes EOF-backed `HoodieIOException`. | The shared hybrid by-name/positional decode fix does not yet cover serialized payloads or several valid evolution shapes. | RED (4 of 5 assertions) |

The reordered-field projection assertion inside the `BaseAvroPayload` aggregate passes; the aggregate is
red because the other four independent assertions fail.

## Exact reproduction commands

Use Maven offline when dependencies are already present. This avoids slow snapshot-metadata network
checks and makes the observed code/test result easier to distinguish from repository availability.

### Positive read and performance checks

```bash
mvn -o -pl hudi-client/hudi-java-client \
  -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip \
  -Dtest=TestHoodieJavaReadClient#testSnapshotAndCompactionRespectEventTimeOrdering test

mvn -o -pl hudi-client/hudi-java-client \
  -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip \
  -Dtest=TestHoodieJavaReadClient#testSnapshotSpillsMergeBufferAndCleansItOnClose test

mvn -o -pl hudi-common \
  -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip \
  -Dtest=TestLazyConcatenatingIterator#testCloseAfterPartialIterationDoesNotOpenRemainingIterators test
```

### Positive write lifecycle checks

```bash
mvn -o -pl hudi-client/hudi-java-client \
  -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip \
  -Dtest=TestHoodieJavaClientOnMergeOnReadStorage#testCleanerPreservesRequestedCompactionFileSlice+testWriteCommitCallbackFiresOnClustering test

mvn -o -pl hudi-client/hudi-java-client \
  -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip \
  -Dtest=TestJavaNonBlockingConcurrencyControl#testNbccRecoversAbandonedInflightCompaction test
```

### Current read gaps: expected build failure

```bash
mvn -o -pl hudi-client/hudi-java-client \
  -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip \
  -Dtest=TestHoodieJavaReadClient#testTimeTravelRejectsScheduledButIncompleteCompactionInstant+testSnapshotReconcilesInternalSchemaRenameInOldLogBlock+testUnsupportedReadConfigurationsAreRejectedAtEntryPoint test
```

Observed summary: 3 tests run, 3 failures. The unsupported-configuration test contains two independent
failed assertions.

### Current write gaps: expected build failure

```bash
mvn -o -pl hudi-client/hudi-java-client \
  -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip \
  -Dtest=TestHoodieJavaClientOnMergeOnReadStorage#testInsertPreppedUsesMorDeltaWritePath+testLogCompactionOnMORTable test
```

Observed summary: 3 invocations, one assertion failure (`insertPrepped`) and two errors (parameterized log
compaction with and without a base file).

### Shared Avro gap: expected build failure

```bash
mvn -o -pl hudi-common \
  -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip \
  -Dtest=TestBaseAvroPayload#getRecordHandlesProjectionSerializationAndEvolutionWithoutDataLoss test
```

Observed summary: one aggregate test fails with four independently reported failures in every
repetition.

## Interpreting the suite

- Do not weaken a red test merely to recover a green build. First decide whether the promised feature
  contract should be implemented or explicitly removed from scope/documented as unsupported.
- For intentional v1 exclusions, fail-fast validation is still valuable: returning a lazy iterator and
  failing later makes the public API misleading and resource behavior harder to reason about.
- The user explicitly requested acceptance tests for later implementation as well. Log-compaction
  rollback and archival contracts are now present, even though their downstream assertions cannot yet
  execute past the unsupported compactor. Do not remove them merely because the initial failure repeats.
- A full module or repository run is expected to fail while the RED tests remain. Passing targeted tests
  establish working behavior; they do not cancel the red evidence.
- Before opening a PR, rerun the Hive/Presto MoR schema-evolution functional suites whenever
  `BaseAvroPayload` behavior changes, because those readers motivated the legacy positional fallback.


## Continuation: operation and edge-case parity audit

The user reaffirmed **tests only, no production edits**. The following additions extend the earlier
hardening phase. Every new method below is in `TestHoodieJavaReadClient` unless another class is named.
A failure is retained as an ordinary regression assertion. Setup failures encountered while developing
fixtures were corrected; they are not counted as product defects.

### Reviewed engine references

These are repository sources reviewed for their storage invariants, not claims that Spark/Flink/Hadoop
suites were executed in this continuation:

- Spark write and table services:
  `hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/client/functional/TestHoodieClientOnMergeOnReadStorage.java`.
- Spark rollback:
  `hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/table/functional/TestHoodieSparkMergeOnReadTableRollback.java`,
  particularly `testRollbackWithDeltaAndCompactionCommit`, `testMORTableRestore`, and
  `testInsertsGeneratedIntoLogFilesRollbackAfterCompaction`.
- Spark restore:
  `hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/functional/TestSavepointRestoreMergeOnRead.java`,
  particularly `testCleaningDeltaCommits` and `testRestoreToWithInflightDeltaCommit`.
- Flink merge, deletes, incremental reads and ordering:
  `hudi-flink-datasource/hudi-flink/src/test/java/org/apache/hudi/table/format/TestInputFormat.java`,
  particularly `testReadBaseAndLogFilesWithDisorderUpdateDelete`, `testReadSkipCompaction`,
  `testReadSkipClustering`, `testMergeOnReadDisorderDeleteMerging`, and the compaction ordering tests.
- Flink log sizing:
  `hudi-flink-datasource/hudi-flink/src/test/java/org/apache/hudi/sink/TestWriteMergeOnRead.java#testWriteMorWithSmallLogBlock`.
- Hadoop/Hive compatibility boundary:
  `hudi-hadoop-mr/src/test/java/org/apache/hudi/hadoop/realtime/TestHoodieMergeOnReadTableInputFormat.java`;
  schema resolution is also covered by the existing shared `TestBaseAvroPayload` regressions.
- Java's public write operations and inherited table operations were inventoried directly from
  `HoodieJavaWriteClient`, `HoodieJavaMergeOnReadTable`, and `HoodieJavaCopyOnWriteTable`.

### Added or expanded cases

| Test | Variants and invariant | Targeted result |
| --- | --- | --- |
| `testRollbackRestoresExactSnapshot` | Four: log-only/base-plus-log × marker/listing rollback. Roll back an inflight append (so marker mode is actually exercised), then a completed insert and update; compare full values, exclude rollback from incremental data, then write and compact again. | PASS |
| `testSavepointRestoreAcrossCompaction` | Two: savepoint a log-only/compacted state, then update, compact, add another partition, leave a writer inflight, restore and write again. | PASS |
| `testDeleteAllAndReinsertAcrossCompaction` | Two file shapes; repeated delete including a missing key, empty compaction output, reinsert, exact incremental result and no duplicate rows. | PASS |
| `testOverlappingSnapshotIteratorsRetainTheirTimeline` | Two file shapes; plan an unconsumed iterator, commit updates and a new partition, read a newer snapshot, then consume the original iterator. | PASS |
| `testWriteOperationLifecycle` | Ten: INSERT, UPSERT, UPSERT_PREPPED, BULK_INSERT, BULK_INSERT_PREPPED × optional base materialization. Validate actual rows, updates, incremental results, deletes and final compaction. | Six PASS; four prepped-update variants RED |
| `testPreppedDeletePreservesOtherKeys` | Two file shapes; tagged tombstone deletes only one key, produces no incremental row and survives compaction. Independent of prepped-upsert failures. | PASS |
| `testSmallLogBlocksAndRolloverPreserveAllRecords` | Six batches of 120 keys over four partitions, 512-byte block target and 1-KiB log target. Assert rollover really occurred, all final values and multiplicity before/after compaction. | PASS |
| expanded `testSnapshotSpillsMergeBufferAndCleansItOnClose` | Eight: BITCASK/ROCKS_DB × log-only/base-plus-log × exhaustion/early close. Force real spill; compare all 100 values on exhaustion; assert directory cleanup for both close paths. | PASS |
| `testEventTimeDisorderedDeletesAndResurrection` | Two file shapes; stale delete ignored, newer delete applied, stale update cannot resurrect a log tombstone, newer update can; compact final winner. | PASS |
| `testCommitTimeOrderingAcceptsLaterCommitWithOlderEventTime` | Two file shapes; commit-time ordering deliberately accepts a lower event timestamp in the later commit. | PASS |
| `testEmptyWriteOperations` | All eight public insert/upsert/bulk/delete entry points including prepped forms. Commit an empty batch without changing existing data or emitting incremental rows. | Seven PASS; empty UPSERT_PREPPED RED |
| `testPartitionScopedKeysRemainDistinctThroughDeleteAndCompaction` | Same key in two partitions remains two rows; deleting one partition/key preserves the other. | PASS |
| `testOverwriteReplaceCommitVisibility` | Four: partition/table overwrite × log-only/compacted setup. Check exact snapshot, multiplicity, time travel, incremental inclusion/skipping, and rollback. Uses the actual Java table API because the write client has no overwrite wrapper. | Log-only variants PASS; compacted variants RED |
| `testMetadataAndFilesystemListingAgreeAfterMorLifecycle` | Metadata disabled/enabled; insert, update, compact and rollback a new partition. Assert enabled metadata was initialized and both listing paths yield exact rows. | PASS |
| `testDuplicateInputUsesEventTimeWinner` | Four: input order × file shape. Precombine repeated keys, retain largest ordering value, preserve nullable data and exactly two rows across compaction. | PASS |
| `testDeletePartitionsPreservesUnselectedPartition` | Log-only/compacted input; desired partition-delete contract at the Java table API. | RED: explicitly unsupported Java operation |
| `TestLazyConcatenatingIterator#testThousandsOfEmptyChildrenKeepAtMostOneReaderOpen` | 10,000 empty readers followed by a nonempty reader; iterative traversal, prior reader closes before the next opens, exact close count. | PASS |
| `TestLazyConcatenatingIterator#testCloseBeforeReadingIsIdempotentAndDoesNotOpenChildren` | Double close before consumption opens no readers; subsequent iteration stays empty. | PASS |
| `TestLazyConcatenatingIterator#testFailingSupplierDoesNotDoubleClosePreviousChild` | Failure to open the next reader must not double-close the exhausted previous reader. | RED |

There are **81 additional test invocations** relative to the handoff: 71 from new read-client
cases, seven extra spill variants replacing the original single spill case, and three iterator cases.
Assertions after an earlier failure are not automatically established: the prepped-upsert lifecycle
cannot currently validate its later delete/compaction assertions. The independent delete test addresses
that particular coverage dependency. Overwrite captures all read results and performs rollback before
asserting them together, so a snapshot mismatch does not prevent checking rollback/time travel.

### Newly exposed implementation gaps

1. **Prepped upsert timeline transition:** update-only and empty prepped upserts leave the instant
   requested rather than inflight. Update-only cases now fail an explicit timeline assertion; the empty
   case fails commit because the inflight file is absent. A new-record prepped upsert may write a base
   file through its bulk-insert branch, so tests do not incorrectly demand a log file in that case.
2. **Overwrite after compaction:** both partition and table overwrite lose the replacement row from
   snapshot and incremental results after a compacted setup. Log-only setup passes. Historical reads,
   skip-overwrite filtering and rollback are checked separately in the same aggregate. This is observed
   behavior; no production root-cause fix was attempted.
3. **Partition delete:** the inherited table method throws `HoodieNotSupportedException`. This is a
   parity gap, not a successful deletion and not disguised as a passing expected-exception test.
4. **Iterator supplier failure:** opening the next child throws after the previous child is closed;
   closing the chain closes that previous child again. The regression expects exactly one close.

### Coverage limits and acceptance gate

This audit does **not** certify literally every edge case or full feature/performance parity. The
following distinctions are required before saying the test work or the feature is complete:

| Area | Remaining scope |
| --- | --- |
| Log compaction rollback/archival | Acceptance tests are present for inflight/completed rollback on both file shapes and archival. Execution is RED; downstream assertions await implementation. |
| Shared schema evolution | Existing Kryo, alias, nested rename and added-default regressions remain RED. The full Java module includes 23 Java file-group-reader cases and 20 Hive file-group-reader cases, all passing. Full Spark/Flink/Presto suites were not run. |
| Standalone bootstrap and partition TTL | Acceptance tests with real Parquet input and aged/fresh partitions are present. Both bootstrap modes and both TTL strategies are RED at the unsupported Java table method. |
| Indexing | Existing Java metadata/index tests remain part of the module suite; this continuation does not claim a Cartesian matrix of all secondary indexes, asynchronous indexing, concurrency modes and table services. |
| Engine-specific features | Spark SQL/Catalyst/vectorization and Flink checkpoint/recommit/source/changelog plumbing have no standalone Java API equivalent. They are outside engine-neutral storage parity. |
| Versions and storage | Updated below: supported versions 6/8/9/10, all 12 directed migrations, advertised base/inline-log formats, native logs and four portable Parquet codecs now have Java lifecycle cases. Vendor object-store behavior is not certified. |
| Fault injection | Updated below: commit/metadata boundaries, abrupt JVM death, persistent/transient storage failures, partial reads with spill cleanup and rollback recovery are executable. Embedded HDFS corruption and DataNode-loss tests now execute. This is a specified boundary matrix, not every machine instruction or vendor failure. |
| Performance | Updated below: deterministic resource ceilings are supplemented by measured/skewed/wide/long-history workloads, allocation and elapsed-time gates, scale controls and an enabled size-estimation workload. These do not measure Spark/Flink speed equivalence. |
| Planning memory | File slices and supplier lists are still eagerly collected. Planning memory grows with selected file groups; the one-reader guarantee covers merge working state only. |

Production files remain unchanged. RED tests must be resolved by a later implementation phase or an
explicitly agreed feature exclusion; passing cases do not erase these gaps.

### Continuation reproduction

The full Java suite needs permission to open localhost sockets for embedded timeline-server tests.
A sandbox `SocketException: Operation not permitted` is environmental and is not a MOR regression.
Use the normal lifecycle so Checkstyle remains enabled:

```bash
mvn -o -pl hudi-client/hudi-java-client -DskipITs test
mvn -o -pl hudi-common -DskipITs \
  -Dtest=TestBaseAvroPayload,TestLazyConcatenatingIterator test
```

Surefire retries can overwrite a class XML with only its failed retry cases. Use Maven's final aggregate
summary and initial per-class summaries for distinct invocation counts; do not sum the final XML files
and call that the full run.


### Acceptance expansion requested during the continuation

The user clarified that unimplemented behavior must still have executable acceptance tests. This
supersedes the earlier handoff recommendation to postpone downstream log-compaction tests.

| Test | Acceptance contract | Final result |
| --- | --- | --- |
| `testLogCompactionRollbackPreservesSnapshot` | Four: log-only/base-plus-log × inflight/completed log-compaction rollback, then a further write and regular compaction. | RED at unsupported log-compaction execution; later assertions are present but not yet reached |
| `testLogCompactionArchivalPreservesSnapshot` | Complete log compaction, advance regular compactions/cleaning, require actual archival of the log-compaction instant and exact live rows. | RED at unsupported log compaction |
| `testMixedMorOperationsAgainstReferenceModel` | Seeds 7, 29, 101, each with 48 transitions: insert/upsert, delete, compact, commit/rollback, and incremental reads. An independent map is compared with snapshot, incremental, read-optimized and multiplicity results after each transition. Captured assertion checks execute together so an early mismatch does not truncate the sequence. | RED: `earliest` incremental reads become empty/partial after compaction and rollback; other captured model assertions passed |
| `testCleanerPoliciesRetainCurrentMorSnapshot` | Commit-count and file-version policies must actually delete obsolete files; hour retention must retain recent files. All preserve exact current rows and allow later writes/compaction. | PASS, all three policies |
| `testClusteringPreservesSnapshotAndIncrementalBoundaries` | Base-only/base-plus-log clustering, exact current/historical rows, default skip-clustering and later incremental updates. | PASS, both shapes |
| `testPartitionTtlRemovesExpiredPartitions` | KEEP_BY_TIME and KEEP_BY_CREATION_TIME must remove an old partition while preserving a fresh partition. | RED: Java table explicitly rejects partition TTL |
| `testBootstrapExternalParquetIntoMor` | Materialize a real external Parquet file; full-record and metadata-only bootstrap must expose the row from the new MOR table. | RED: Java table explicitly rejects bootstrap |
| `testManyFileGroupsKeepOneSpillBufferOpen` | Both BITCASK and ROCKS_DB, 512 keys, 16 partitions, four buckets, two batches; require at least 32 file groups, zero spill buffers during planning, exactly one while reading, zero after close and all final rows without duplicates. | PASS, both disk maps |
| `testAddedSchemaFieldDefaultAcrossOldBaseAndLogs` | Add a field with an Avro default; old base/log records obtain the default, new records retain their supplied value, and compaction preserves both. | PASS, both shapes |
| `testSnapshotWithoutMetadataFields` | Meta-field-free MOR snapshot merging and read-optimized compaction with reconstructed record keys. Separate from the unsupported incremental contract. | PASS, both shapes |

The fixed-seed model is an additional oracle, not a random timing/stress test. Each seed executes all
48 transitions and checks over 180 captured assertions. The observed failure is the documented
`earliest` incremental/snapshot equivalence after compaction and rollback; the regular snapshot and
read-optimized models continue to be checked even after an incremental mismatch.

### Existing shared coverage retained in the acceptance suite

The complete Java module already executes concrete Java and Hive subclasses of
`TestHoodieFileGroupReaderBase`. They cover record merge modes, base/log formats, metadata population,
multiple ordering fields, mixed writer schemas, bootstrap reads and spillable maps. These are shared
reader contracts and do not need to be copied into another Java-client integration test to count as
coverage. The full-module run passed all 23 Java and 20 Hive invocations, as well as all 68 Java metadata
cases and 20 NBCC cases.

Additional shared verification targets corruption/rollback log scanning in
`hudi-hadoop-common/.../TestHoodieLogFormat`, hour-retention boundaries in
`hudi-common/.../TestCleanerUtils`, and mandatory/projection schemas in
`hudi-common/.../TestFileGroupReaderSchemaHandler`. Their final results are recorded below.

```bash
mvn -o -pl hudi-client/hudi-java-client -DskipITs -Dtest=TestHoodieJavaReadClient test
mvn -o -pl hudi-common -DskipITs \
  -Dtest=TestCleanerUtils,TestFileGroupReaderSchemaHandler,TestBaseAvroPayload,TestLazyConcatenatingIterator test
mvn -o -pl hudi-hadoop-common -DskipITs \
  -Dtest=TestHoodieLogFormat#testAppendAndReadOnCorruptedLog+testAvroLogRecordReaderWithMixedInsertsCorruptsAndRollback+testAvroLogRecordReaderWithMixedInsertsCorruptsRollbackAndMergedLogBlock+testLogFileReaderReadsPastCorruptBlock test
```


## Earlier verification (2026-09-05; superseded by the remaining-gap continuation below)

| Run | Distinct cases | Passed | Assertion failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full `hudi-java-client` module before the final acceptance expansion | 340 | 325 | 10 | 5 | 0 |
| Final `TestHoodieJavaReadClient`, including all added acceptance cases | 104 | 80 | 12 | 12 | 0 |
| Shared `TestCleanerUtils,TestFileGroupReaderSchemaHandler,TestBaseAvroPayload,TestLazyConcatenatingIterator` | 103 | 101 | 2 | 0 | 0 |
| Selected `TestHoodieLogFormat` corruption cases | 0 | **Not executed** | — | — | — |

The full-module run included an earlier 81-case version of the read-client class. The final class run
replaced that validation after the last additions and fixture corrections; it is not additive to all
340 cases. Together the two Java runs cover **363 distinct Java-module invocations: 336 passing and 27
failing**. This is coverage across two runs, not a claim that a single final full-module run had that
summary. The 27 consist of 24 read-client acceptance failures plus the three MOR write/table-service
failures from the handoff.

The 24 final read-client failures are fully accounted for:

- 4 update-only prepped-upsert variants fail the requested-to-inflight transition assertion.
- 1 empty prepped upsert fails commit because its inflight instant does not exist.
- 2 compacted overwrite variants lose replacement rows in snapshot/incremental results.
- 3 fixed-seed model cases find empty/partial earliest incremental results after compaction/rollback.
  All 48 transitions execute for each seed; the aggregates contain 7, 8 and 8 incremental mismatches.
- 1 InternalSchema rename loses the renamed value.
- 1 time-travel case accepts an incomplete compaction target.
- 1 unsupported-configuration aggregate contains the no-meta incremental and LSM validation gaps.
- 2 partition-delete cases, 2 TTL strategies and 2 bootstrap modes reach explicitly unsupported methods.
- 4 log-compaction rollback cases and 1 archival case stop at unsupported log-compaction execution.

The final shared failures are the existing `BaseAvroPayload` schema/serialization aggregate (four
independent mismatches) and the new iterator supplier-failure double-close regression.

The corruption command's BUILD SUCCESS is **not** validation: `HoodieTestUtils.useExternalHdfs()` aborts
class setup with an assumption unless `-Duse.external.hdfs=true` is supplied. It also requires a running
HDFS service at `hdfs://localhost:9000`. No external HDFS service was started here, and no corruption case
was counted as passing. With that service available, add `-Duse.external.hdfs=true` to the documented
corruption command. The test definitions remain in the repository as part of the shared storage suite.

Checkstyle passed for the final Java and common runs; `git diff --check` passed. Working-tree changes
are confined to test Java files and the two evidence/handoff Markdown files. No production source was
changed, no regression was disabled, and expected-exception assertions were not substituted for the
unimplemented positive contracts.

Local run logs (temporary, useful for detailed diagnosis):

- `/tmp/java-mor-parity-full-module.log`
- `/tmp/java-mor-parity-final-read.log`
- `/tmp/java-mor-parity-shared-contracts.log`
- `/tmp/java-mor-parity-corruption.log`


## Remaining-gap continuation (2026-09-05)

The user asked to address all the previously identified categories, while retaining the **tests-only**
constraint. The additions below are executable positive acceptance contracts. Expected exceptions are
used only when the contract itself requires a conflict or injected-fault notification; unsupported
positive features remain ordinary failing tests. Production code and build configuration are unchanged.

### Explicit coverage matrix

| Area | Executable cases and checks |
| --- | --- |
| Supported table versions | `testLegacyVersionSixStorageLifecycle`: nine base/log pairs for v6, using single-writer INMEMORY (NBCC is not a v6 contract). Snapshot, updates, deletion, rollback and compaction preserve values and the version. |
| Modern storage combinations | `testStorageCompatibilityLifecycle`: 18 combinations of v8/v9 × PARQUET/ORC/HFILE base × avro/parquet/hfile inline blocks, plus three v10 native formats. Snapshot/history, two partitions, exact multiplicity, delete/rollback, incremental latest-write selection and compaction. Requires real logs and checks native-vs-inline filenames. V10 uses native logs derived from the base format; inline-block settings are not counted as extra v10 coverage. |
| Compression | `testParquetCodecLifecycle`: v8/v9/v10 × UNCOMPRESSED/SNAPPY/GZIP/ZSTD. Runs the lifecycle and checks codec names in actual file footers. Codec libraries outside these four portable choices, such as optional Brotli/LZO installations, are not claimed. |
| Upgrade/downgrade | `testTableVersionTransitionPreservesDataAndFurtherWrites`: all 12 directed transitions among supported write versions 6, 8, 9, 10. Starts with real updated MOR data; verifies values, version, further writes, applicable incremental reads and compaction. Reopens metadata after a layout migration instead of reusing a stale layout version. Versions 0–5/7 are not advertised write versions in `HoodieWriteConfig.WRITE_TABLE_VERSION`. |
| Commit and metadata boundary failures | `testCommitFailureRecovery`: 12 combinations of before-MDT / after-MDT / after-data-commit failure × log-only/compacted × marker/listing rollback. The after-MDT case performs the actual MDT commit before throwing. Reopens clients, checks commit visibility, rolls back incomplete writes, retries and compares metadata/filesystem results. |
| Abrupt process death | `testAbruptWriterProcessDeath`: four combinations of log-only/compacted × before/after data commit. A separate JVM calls `Runtime.halt(73)` without close or shutdown hooks. The parent requires that exit code, verifies visibility, recovers and compacts. |
| Storage write failure | `testStorageWriteFailureCanRollbackAndRetry`: CREATE/WRITE/CLOSE × transient/persistent. Uses a test Hadoop filesystem so native Parquet I/O is intercepted too; disables the plain-file cache and refreshes the hoodie-file wrapper before each fault fixture and excludes partition metadata from data-fault targeting. Requires evidence of injection, checks incomplete-write invisibility, rolls back and retries, then checks the error-reporting contract. |
| Storage read failure | `testReadIoFailureIsVisibleAndReaderCanRetry`: log-only and compacted input; persistent I/O errors must reach the caller and the same reader must work once the fault is removed. |
| Partial reads and cleanup | `testReadFailureAfterPartialResultsClosesSpillAndAllowsRetry`: two shapes, two file groups, real one-byte-budget spill, consume a row before failing the unread partition. The error must propagate, close must remove spill files, and retry returns both exact rows. |
| Data rollback interruption | `testRollbackRetriesAfterDataFileDeleteFailure`: inject an actual data-file delete error during rollback, remove it, retry the same rollback, verify the original snapshot and a subsequent write. |
| Metadata rollback interruption | `testRollbackRecoversAfterMetadataStorageFailure`: CREATE/WRITE/DELETE during MDT rollback after a real post-MDT failed data commit. Requires actual injection and retry, then verifies recovered data and metadata/filesystem agreement. |
| Iterator exceptional paths | `TestLazyConcatenatingIterator`: hasNext/next failure preserves the same cause and closes only initialized readers; a throwing close must leave the chain disposed and idempotent. Adds three invocations alongside the earlier supplier-failure and 10,001-reader tests. |
| Optimistic concurrency | `testOptimisticConcurrentWritersPreserveCommittedRows`: four combinations of overlapping/disjoint groups × log-only/compacted. Requires an overlap conflict, success for disjoint groups, exact committed values, no duplicate rows, rollback/retry and compaction. Supplements the existing NBCC cases and no-deadlock multiple-writer test. |
| Shared schema evolution | `TestHoodieFileGroupReaderOnJava` now leaves `addNewFieldSupport` enabled. The inherited engine-neutral schema-evolution tests no longer silently omit added fields. |
| Shared bulk-insert fixture | `HoodieFileGroupReaderOnJavaTestBase` now recognizes `bulkInsert` case-insensitively; the previous lowercase comparison to mixed case could only route it to upsert. |
| Real HDFS corruption | Twelve selected `TestHoodieLogFormat` invocations cover corrupt append/tails, reading past corrupt blocks, mixed corrupt/inserts/rollback and merged blocks on disk-map variants. Opt-in embedded HDFS now reaches setup and starts one cluster instead of starting twice. |
| DataNode failure | `TestHoodieLogFormatAppendFailure` runs a real four-DataNode cluster, removes the nodes holding a block, and checks append recovery/rollover. Enabled with the same embedded-HDFS flag. |
| Shared storage behavior | `TestFSUtilsWithRetryWrapperEnable`, `TestHoodieRetryWrapperFileSystem`, `TestExternalSpillableMap`, `TestBitCaskDiskMap`, `TestRocksDbDiskMap`: retries/delegation, both disk maps and compression variants. |
| Measured performance | `testMeasuredMorWorkload`: four row-count/width/skew shapes, eight versions per key, warmup and repeated exact-result scans, compaction, elapsed times, scan throughput and main-thread allocation counters. Configurable time/allocation guards. `TestBitCaskDiskMap#testSizeEstimatorPerformance` is enabled with warmup and 10,000 estimates instead of a disabled cold-start 100ms assertion. |

### Performance execution and interpretation

The measured Java cases run by default, are tagged `performance`, and emit one file per workload under
`hudi-client/hudi-java-client/target/mor-performance/`. The defaults cover 256/2,048 keys, 16/4,096-character
values, 16 partitions, eight write batches and repeated scans. `heap_delta_bytes` is an observation that
can decrease with GC (reported as zero); it is not a peak-heap or leak proof. `main_thread_allocated_bytes`
is cumulative allocation on the test thread, not allocation by all native/background threads. The suite
also independently enforces actual spill/cleanup and one-active-buffer behavior.

Default guards are 180 seconds per workload and 8 GiB of main-thread allocations. They are generous
regression ceilings, not service-level objectives or claims of Spark/Flink performance equivalence.
Use `-Djava.mor.performance.scale=N` for larger key counts, with explicit matching
`-Djava.mor.performance.maxSeconds=...` and `-Djava.mor.performance.maxAllocatedBytes=...` budgets. The
size-estimation gate accepts `-Djava.mor.estimator.maxMillis=...` (default 5,000 ms). Scale runs use the
same correctness assertions; no reduced-check benchmark mode exists.

### Reproduction for the expanded matrix

```sh
mvn -o -pl hudi-client/hudi-java-client -DskipITs test
mvn -o -pl hudi-common -DskipITs \
  -Dtest=TestCleanerUtils,TestFileGroupReaderSchemaHandler,TestBaseAvroPayload,TestLazyConcatenatingIterator test
mvn -o -pl hudi-hadoop-common -DskipITs -Duse.embedded.hdfs=true \
  '-Dtest=TestHoodieLogFormat#testAppendAndReadOnCorruptedLog+testAvroLogRecordReaderWithMixedInsertsCorruptsAndRollback+testAvroLogRecordReaderWithMixedInsertsCorruptsRollbackAndMergedLogBlock+testLogFileReaderReadsPastCorruptBlock+testSkipCorruptedCheck+testValidateCorruptBlockEndPosition' test
mvn -o -pl hudi-hadoop-common -DskipITs -Duse.embedded.hdfs=true \
  -Dtest=TestHoodieLogFormatAppendFailure test
mvn -o -pl hudi-hadoop-common -DskipITs \
  -Dtest=TestHoodieRetryWrapperFileSystem,TestFSUtilsWithRetryWrapperEnable,TestExternalSpillableMap,TestBitCaskDiskMap,TestRocksDbDiskMap test
```

Local server/socket access is required for HDFS and existing timeline-server tests; Mockito requires
JVM attach access. Sandbox-only failures were rerun with those permissions and excluded from defect
counts. No Docker containers or downloaded images were needed; unrelated running containers were only
listed. HDFS clusters are started and stopped by the test fixtures.

### Meaning of completion

This matrix defines the bounded acceptance scope: Java MOR APIs and engine-neutral storage/service
contracts, with explicit supported versions/formats and failure boundaries. It does not certify every
Cartesian product of optional indexes, all storage vendors, every crash instruction, or engine-specific
Spark SQL/Flink checkpoint APIs. No finite suite can substantiate that broader literal claim. Failing
positive contracts and later assertions blocked by those failures remain implementation work. Do not
report the MOR feature as complete while they remain red.


### New implementation gaps exposed in the remaining-gap matrix

- **Six mixed-format cases:** a Parquet inline log block is decoded using the ORC/HFILE base-file
  reader in the v6/v8/v9 matrices. The reader reports malformed ORC or invalid HFile magic. Other
  base/log combinations and v10 native lifecycles pass. The tests keep the intended successful read
  and later lifecycle assertions.
- **Two persistent data I/O cases:** CREATE and WRITE injections occur, but insertion can return
  without an exception or a `WriteStatus` marked erroneous. Rollback/retry correctness is checked
  before the final error-notification assertion; the positive contract is left red.
- **One metadata rollback recovery case:** after persistent MDT DELETE failure is removed, retrying
  the rollback fails with `HoodieMetadataException: Failed to rollback deltacommit`. CREATE/WRITE
  interruption recovery passes.
- **One additional shared iterator case:** when child `close()` throws, a second close retries that
  same child and throws again instead of leaving the chain disposed. This supplements the earlier
  supplier-failure double-close regression.

These are additional to the earlier prepped-upsert, overwrite, incremental-model, schema/payload,
time-travel validation, bootstrap, TTL, partition-delete and log-compaction failures. All supported
version migrations and enabled shared added-field schema cases passed in their targeted verification.


### Final verification of the remaining-gap continuation

| Run | Cases | Passed | Assertion failures | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full Java module before the last OCC/schema/cache-fixture refinements | 451 | 415 | 15 | 21 | 0 |
| Final reader class, including all 92 additional Java invocations | 196 | 163 | 14 | 19 | 0 |
| Shared Java file-group reader with added fields enabled | 23 | 23 | 0 | 0 | 0 |
| Common schema/cleaner/payload/iterator contracts | 106 | 103 | 2 | 1 | 0 |
| Embedded HDFS corruption/rollback selection | 12 | 12 | 0 | 0 | 0 |
| Final DataNode-failure and BitCask class run | 11 | 11 | 0 | 0 | 0 |
| Other selected shared storage classes (retry utilities, external map, RocksDB, retry wrapper) | 140 | 140 | 0 | 0 | 0 |

The full Java run reported four retry-pass flakes caused by the test filesystem wrapper retaining an
old non-faulting filesystem. The final fixture explicitly refreshes that wrapper, and the final
196-case reader run has **no flakes and no skipped cases**: the same 33 implementation failures occur
on the initial attempt and all three retries. The wrapper remains cached *within* a test because
HFile metadata bootstrap needs that instance's stream-byte accounting. The final schema class was
verified independently after removing its added-field suppression.

Replacing those two class results in the module coverage yields **455 distinct final Java cases:
419 passing and 36 failing**. This is combined verification across the full-module and final-class
runs, not a claim of a single final 455-case module execution. The 36 are the 33 reader failures plus
three MOR write/service failures. The shared/HDFS runs cover **269 distinct cases: 266 passing and
three failing**. The aggregate default coverage is therefore **724 cases: 685 passing and 39 failing**;
parameter rows are distinct cases, while individual assertions within `assertAll` are not counted as
separate cases. Measured default performance cases are included in the Java count, not added twice.

This phase added **92 Java invocations and three common iterator invocations**, re-enabled one existing
BitCask performance invocation and expanded the assertions in the existing shared schema cases. All
12 supported-version migrations, four strong OCC cases, four abrupt-process-death cases and 12
commit-boundary cases pass. The 13 HDFS corruption/DataNode cases now actually execute.

Normal Maven Checkstyle/RAT checks and `git diff --check` pass. The working tree changes are confined
to test Java files and these evidence/handoff documents. No production source or POM was changed.

Final local logs:

- `/tmp/java-mor-parity-full-module.log` — full module before final fixture refinements.
- `/tmp/java-mor-parity-final-read.log` — final 196-case reader class, no flakes.
- `/tmp/java-mor-final-occ-schema.log` — shared added-field schema class: 23 passing; its initial OCC
  compacted fixture errors were corrected and the four OCC cases pass in the final reader run.
- `/tmp/java-mor-parity-shared-contracts.log` — final common contracts: 106 cases.
- `/tmp/java-mor-embedded-corruption.log` — 12 actual HDFS cases.
- `/tmp/java-mor-final-hdfs-perf.log` — final DataNode/BitCask: 11 passing, no skips.
- `/tmp/java-mor-storage-contracts.log` — other storage classes pass; its historical disabled BitCask
  row is replaced by the final 10-case BitCask class result above.


### Larger measured workload verification

The same four performance shapes also pass at `java.mor.performance.scale=4`: 1,024/8,192 keys,
16/4,096-character values, eight write batches (up to 65,536 record versions per case), repeated exact
snapshot scans and compaction. This is an additional workload-size run, not four extra default-suite
definitions. **Four ran, four passed, none skipped** in 26.27 seconds total on this machine, under the
explicit 180-second-per-case and 32-GiB-main-thread-allocation guards.

```sh
mvn -o -pl hudi-client/hudi-java-client -DskipITs \
  '-Dtest=TestHoodieJavaReadClient#testMeasuredMorWorkload' \
  -Djava.mor.performance.scale=4 \
  -Djava.mor.performance.maxSeconds=180 \
  -Djava.mor.performance.maxAllocatedBytes=34359738368 test
```

Log: `/tmp/java-mor-scaled-performance.log`. Measurements for both default and scaled shapes are under
`hudi-client/hudi-java-client/target/mor-performance/`; workload filenames contain the actual scaled row
count. The Java process uses the repository's Java 17 profile (4-GiB heap, Parallel GC). These numbers
are local end-to-end Java measurements and are not a benchmark comparison with Spark or Flink.


## Implementation verification (2026-09-05; current phase)

Production changes are now authorized. The earlier RED rows describe the test-only baseline and
are superseded by this verification. No failing acceptance case was disabled or changed to accept
an unsupported implementation. Two necessary fixture corrections are documented below.

### Implemented behavior

- Prepared inserts use MOR delta executors; prepared upserts publish recoverable inflight metadata
  before writing, including update-only and empty batches.
- Inserts use INSERT operation semantics and the insert-specific combine switch. A new two-row
  parameterized regression failed with the old upsert switch in both configurations and passes
  after correction, while retaining event-time winner assertions.
- Log compaction executes in Java, and rollback finds pending log compaction on the write timeline.
  Pending regular compaction remains excluded from this generic rollback path; see the Spark
  regression correction below.
- Overwrites create new file groups instead of reusing the groups that the replace commit hides.
- MOR small-file planning follows Spark: it considers log-only groups, estimates base-plus-log size,
  excludes pending compaction, and avoids rewriting a base file with uncompacted logs when the index
  cannot index logs. This preserves file-group packing with correct INSERT semantics.
- Partition deletion uses replace commits and shared pending-service conflict checks. TTL uses the
  shared retention strategies and commits its result for the inline table-service caller.
- Full and metadata-only external-file bootstrap, subsequent update/compaction, and bootstrap rollback
  work. Metadata-only bootstrap projects record-key columns; files are processed one at a time.
  Rollback preserves the external source. Mode selection and unsupported provider validation happen
  before bootstrap writes.
- Java reads validate completed time-travel targets and unsupported configurations, reconcile internal
  schemas, and return the current snapshot for an unbounded earliest incremental query, including
  after compaction and rollback. Internal schema resolution is shared across the slices in one read.
- Inline Parquet log blocks use the Parquet decoder even when base files are ORC/HFile. Native log
  files retain their native decoder.
- Payloads retain their original writer schema across projections and Kryo/Java serialization, resolve
  aliases, added defaults, nested legacy renames and matching named-union branches. Legacy Kryo bytes
  can still be read when the caller supplies their writer schema first.
- Failed first append writes retain their error status even when no append result was produced.
  Iterator closure is idempotent even if a child close or the next supplier throws.
- Metadata writers inherit configured heartbeat leases. Recovery waits for a stopped owner's
  undeleted lease to expire before another writer takes over.

### Fixture corrections and added regression coverage

Bootstrap previously inherited `_row_key`/`partition_path` even though its source schema has `id`/`part`.
The corrected fixture configures the real fields, then checks update, compaction, rollback, and external
file preservation as well as the initial read. TTL now asserts that its action is auto-committed,
matching Spark and the inline-service caller.

The metadata DELETE failure can prevent heartbeat-file cleanup. Immediate takeover would violate
concurrency protection. The fixture now uses a 1-second heartbeat with two tolerated misses and waits
for actual lease expiry before retrying; the positive recovery and data/listing assertions remain.
A shared configuration test verifies propagation of these settings to the metadata writer.

Additional payload tests cover Java serialization after projection, legacy Kryo input, and evolution
of a union with multiple differently named record branches.

### Verified runs

| Scope | Cases | Result | Evidence log |
| --- | ---: | --- | --- |
| Full Java-client module before the final two insert-combine cases | 455 | PASS, zero failures/errors/skips | `/tmp/java-mor-final-full-java.log` |
| Final reader + MOR functional + NBCC, performance scale 4 | 231 | PASS, zero failures/errors/skips | `/tmp/java-mor-final-reader-scale4.log` |
| Shared schema/iterator and primary payload regressions | 138 | PASS | `/tmp/java-mor-common-final.log` |
| Broader Avro payload families and reader contexts | 88 | PASS; 39 overlap the previous row | `/tmp/java-mor-avro-regressions.log` |
| Shared write, rollback, heartbeat, metadata configuration | 65 | PASS | `/tmp/java-mor-shared-write-regressions.log` |
| Hadoop/HDFS corruption, DataNode loss, spill, filesystem retries | 163 | PASS, zero skips | `/tmp/java-mor-final-hadoop.log` |
| First full run after insert correction, performance scale 16 | 457 | 451 pass; 6 log-only small-file assertions exposed the missing MOR partitioner | `/tmp/java-mor-final-all-scale16.log` |
| Reader, MOR functional, NBCC, Java/Hive file-group readers after MOR planner fix | 274 | PASS, zero failures/errors/skips | `/tmp/java-mor-final-planner-regressions.log` |
| Final full Java-client module after MOR planner fix, performance scale 16 | 457 | PASS, zero failures/errors/skips | `/tmp/java-mor-final-green-scale16.log` |

The non-Java rows contain **415 distinct parameter invocations**, after removing the overlap.
The final full Java module passed **457 cases**, including the two new insert-combine cases, in
9 minutes 38 seconds, finishing at `2026-09-05T20:12:56-04:00`. Together these suites validate
**872 distinct cases with zero failures, errors, or skips**. This is the full Java-client module plus
the selected shared contracts above, not a claim that every test in the entire Hudi repository ran.
The 274-case targeted run overlaps the full Java run and is not counted again.

Shared modules were rebuilt and installed before dependent Java-client runs. The final schema-union
adjustment was verified in both common runs and in the final MOR/reader rerun. The final full module
also verifies the last bootstrap projection and validation changes together with all other Java tests.

### Performance evidence and limits

Environment: OpenJDK 17.0.20.1, local filesystem for measured Java workloads, default Surefire
4-GiB heap (`-Xmx4g`, Parallel GC). HDFS failures were tested using an embedded cluster separately.
The scale-4 run used an explicit 180-second per-case budget and 32-GiB cumulative main-thread
allocation budget. Main-thread allocation is not peak heap or whole-process allocation.

Scale-4 measurements (8 write batches, warm-up, 6 measured scans, then compaction and result checks):

| Keys | Value width | Skewed | Write ms | Six scans ms | Compaction ms | Scan rows/s |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 8,192 | 16 | true | 1,129 | 2,225 | 153 | 22,081.79 |
| 8,192 | 4,096 | false | 2,335 | 4,248 | 200 | 11,570.30 |

Final scale-16 measurements use the same 4-GiB heap, a 180-second per-case budget, and a
128-GiB cumulative main-thread allocation budget. The largest case writes 262,144 row versions
across eight batches for 32,768 distinct keys; its value width is 4,096 bytes.

| Keys | Value width | Skewed | Write ms | Six scans ms | Compaction ms | Scan rows/s |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 4,096 | 16 | false | 1,739 | 4,211 | 196 | 5,834.77 |
| 4,096 | 4,096 | true | 1,415 | 2,192 | 155 | 11,207.30 |
| 32,768 | 16 | true | 2,188 | 3,432 | 401 | 57,283.31 |
| 32,768 | 4,096 | false | 5,765 | 5,331 | 287 | 36,876.37 |

The largest wide case recorded 18,541,982,816 cumulative main-thread allocated bytes and a
702,352,984-byte end-minus-start heap increase. These are not peak-memory measurements. Raw reports
are in `hudi-client/hudi-java-client/target/mor-performance/`; subsequent runs with the same parameters
overwrite them, so the measurements above preserve this run's evidence.

These measured workloads validate correctness under size, skew and history pressure, with explicit
regression bounds. Values are repetitive/compressible and reads are local and warmed; the measurements
are not Spark/Flink benchmarks, object-store SLAs, a sustained-load soak, or production throughput
promises. File-group merge buffering is lazy, but planning still retains a list of selected file slices.
Java write APIs accepting lists require the caller's batch to fit in memory.

Supported-mode readiness is distinct from full engine feature parity. Archived completion-time
incremental ranges, LSM-tree reads, and custom full-bootstrap input-provider classes remain explicitly
unsupported. CDC writes are covered through the shared extractor; this reader has no CDC-read method.
The new schema-bearing Kryo representation requires consistent library versions across producers and
consumers; an older reader cannot consume the new representation. Legacy schema-less bytes cannot
reconstruct an unknown original schema.

The user has not supplied a production storage backend, workload volume, latency/throughput target,
or soak duration. Production certification beyond the validated Java 17/local/HDFS profile remains
unestablished.

### Reproduction of implementation verification

Build/install the current dependency artifacts first:

```bash
mvn -pl hudi-client/hudi-java-client -am -DskipTests -DskipITs install
```

Run the entire Java module with the large workload gate:

```bash
mvn -pl hudi-client/hudi-java-client -DskipITs \
  -Djava.mor.performance.scale=16 \
  -Djava.mor.performance.maxSeconds=180 \
  -Djava.mor.performance.maxAllocatedBytes=137438953472 test
```

Common contracts and Avro regressions:

```bash
mvn -pl hudi-common -DskipITs \
  '-Dtest=TestCleanerUtils,TestFileGroupReaderSchemaHandler,TestLazyConcatenatingIterator,Test*Payload,TestHoodieAvroReaderContext,TestHoodieReaderContext' test
```

Shared write/recovery contracts:

```bash
mvn -pl hudi-client/hudi-client-common -DskipITs \
  -Dtest=TestBaseHoodieTableServiceClient,TestHoodieNativeLogAppendHandle,TestHoodieAppendHandle,TestFileGroupReaderBasedNativeLogAppendHandle,TestHoodieMetadataWriteUtils,TestHoodieHeartbeatClient test
```

Hadoop/HDFS contracts:

```bash
mvn -pl hudi-hadoop-common -DskipITs -Duse.embedded.hdfs=true \
  '-Dtest=TestHoodieLogFormat#testAppendAndReadOnCorruptedLog+testAvroLogRecordReaderWithMixedInsertsCorruptsAndRollback+testAvroLogRecordReaderWithMixedInsertsCorruptsRollbackAndMergedLogBlock+testLogFileReaderReadsPastCorruptBlock+testSkipCorruptedCheck+testValidateCorruptBlockEndPosition,TestHoodieLogFormatAppendFailure,TestHoodieRetryWrapperFileSystem,TestFSUtilsWithRetryWrapperEnable,TestExternalSpillableMap,TestBitCaskDiskMap,TestRocksDbDiskMap' test
```

The actual runs used Maven offline mode because dependencies were cached. No checkstyle, RAT,
Spotless, or acceptance-test assertions were bypassed. Embedded HDFS and some JVM instrumentation
require local socket/attach access; a restrictive process sandbox must permit that test activity.

### Follow-up: root-build timezone failure

The user reported `TestHoodieActiveTimeline#testParseDateFromInstantTime` failing during root
`mvn clean test`: expected `1609502461123`, observed `1609520461123`. The parser and this test
were unchanged from pre-MOR commit `8e88496082`. The test hardcoded a UTC epoch while the parser
uses the JVM's local timezone; Toronto's January offset accounts for the exact five-hour difference.
The failure reproduces in isolation before the fix (`/tmp/hudi-timeline-timezone-before.log`).

An initial test-only correction constructed the expected local date/time explicitly, including its
123-millisecond fraction, then converted it using the system zone. Production parsing was unchanged.
With that temporary correction, all 22 tests in the class passed in each of `America/Toronto`, `UTC`,
and `Asia/Kolkata`; logs are
`/tmp/hudi-timeline-timezone-toronto.log`, `/tmp/hudi-timeline-timezone-utc.log`, and
`/tmp/hudi-timeline-timezone-kolkata.log`.

The complete affected module also passed a clean build:

```bash
mvn -o -pl hudi-hadoop-common -Duser.timezone=America/Toronto clean test
```

Result: **1,272 tests, zero failures/errors, 3 skips**, in 2 minutes 9 seconds, recorded in
`/tmp/hudi-hadoop-common-timezone-clean.log`. This follow-up did not rerun the entire root reactor.

The user subsequently requested environment alignment instead of changing existing tests for this
issue. The timezone test correction above was removed; `TestHoodieActiveTimeline.java` is identical
to HEAD again. Use UTC for local runs to satisfy the original test's existing expectation:

```bash
TZ=UTC mvn -Duser.timezone=UTC clean test
```

To use the affected CI job's test and engine profiles:

```bash
TZ=UTC mvn -B -ntp -Punit-tests,warn-log \
  -Dscala-2.12 -Dspark3.5 -Dflink2.2 \
  -Duser.timezone=UTC -Dgpg.skip -Djacoco.skip=false \
  -fae -pl hudi-hadoop-common clean test
```

The relevant workflow is `.github/workflows/bot.yml`, job
`test-spark-client-and-hadoop-common`. That job uses Ubuntu and Temurin 11; this machine currently
has Arch Linux and OpenJDK 17, so matching the profiles and timezone does not reproduce the full
runner environment. The workflow does not explicitly set a timezone, and the specific pushed run's
timezone has not been verified. UTC is selected here because the unchanged assertion requires it.
The root command above is a local recipe, not the entire CI workflow: CI separates builds, unit tests,
functional tests, and integration tests. Matching the CI `unit-tests` profile excludes functional tags.

The affected-module command passed with the original timezone test restored: **1,272 tests,
zero failures/errors, 3 skips**, including all 22 `TestHoodieActiveTimeline` cases. The Surefire report
confirms `user.timezone=UTC`. Evidence: `/tmp/hudi-hadoop-common-ci-utc.log`. Dependencies absent from
the local cache were downloaded; no quality checks were disabled. The local run used the available
JDK 17 and Maven 3.9.16 and did not execute the full CI dependency-build stage or root reactor.

### Follow-up: Spark-client failures in the root run

The user's subsequent UTC root run reached `hudi-spark-client` and reported 965 tests, 4 failures,
18 errors, and 12 skips. This exposed both local environment requirements and a regression in our
shared implementation. Existing Spark tests and assertions were kept unchanged.

- Sixteen errors traced to Kryo reflecting into `java.util.concurrent.locks`, which the existing
  Java 17 Maven profile does not open. Adding
  `--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED` to the JVM environment resolved them.
- Two ZooKeeper tests failed because the embedded server's admin endpoint could not bind port 8080;
  the XML report contains `Failed to bind to /0.0.0.0:8080`, followed by client connection failures.
  `-Dzookeeper.admin.serverPort=0` assigns an ephemeral admin port without stopping other services
  or disabling the lock tests.
- Four parameter rows of `TestClientRollback#testFailedRollbackCommit` remained failing after those
  environment fixes. Our change from the commits timeline to the write timeline included pending
  regular compaction as well as log compaction. The generic rollback then created an extra rollback
  instant. Both lookup sites in `BaseHoodieTableServiceClient` now exclude `COMPACTION_ACTION`,
  retaining the separate regular-compaction rollback path and Java log-compaction support.

Verification gap: the earlier implementation phase ran the full Java module and selected shared
contracts, but did not run the Spark rollback tests after changing shared rollback behavior. The
existing Spark assertions detect the interaction missed by those suites. Environment-only reruns
isolated the four rollback failures from the 18 errors before the implementation was corrected.

Evidence from existing tests:

| Run | Result | Log |
| --- | --- | --- |
| Environment fixes only; current implementation before correction | 46 tests, 42 pass, 4 rollback failures, zero errors/skips | `/tmp/hudi-spark-environment-regressions.log` |
| Corrected implementation; unchanged Spark rollback assertion | 4 pass, zero failures/errors/skips | `/tmp/hudi-spark-rollback-corrected.log` |
| Shared write, rollback, heartbeat and metadata contracts | 65 pass, zero failures/errors/skips | `/tmp/hudi-shared-rollback-correction.log` |
| Java reader, MOR functional and NBCC contracts | 231 pass, zero failures/errors/skips | `/tmp/hudi-java-rollback-correction.log` |
| Full Spark-client CI unit-test profile | 919 tests, zero failures/errors, 12 skips; BUILD SUCCESS in 9m31s | `/tmp/hudi-spark-full-environment-corrected.log` |
| Spark-client functional profile, excluded from unit-test profile | 46 tests, zero failures/errors/skips | `/tmp/hudi-spark-functional-environment-corrected.log` |

The two Spark profiles together cover the original **965 cases: 953 passed, 12 skipped, zero
failures/errors**. They ran sequentially with the same corrected shared implementation and JVM/ZooKeeper
settings. No previously failing case was skipped or changed. The six functional classes excluded by
the unit-test profile were verified in the functional run, including savepoint/restore, metadata
bootstrap, cleaning/archiving, and marker-based rollback.

For local Java 17 root runs, retain the UTC setting and add the two verified environment settings:

```bash
TZ=UTC \
JDK_JAVA_OPTIONS="${JDK_JAVA_OPTIONS:-} --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED" \
mvn -Dscala-2.12 -Dspark3.5 -Dflink2.2 \
  -Dmaven.test.additionalClasspath="$HOME/.m2/repository/org/apache/hadoop/hadoop-client-runtime/3.3.5/hadoop-client-runtime-3.3.5.jar" \
  -Duser.timezone=UTC -Dzookeeper.admin.serverPort=0 -fae clean test
```

These settings preserve the repository's existing Maven `argLine`, including its other module
openings and heap settings. The entire root reactor has not been verified with this command.

### Follow-up: missing Spark/Flink adapter artifacts

The next root run failed resolving `hudi-spark3.5.x_2.12:1.3.0-SNAPSHOT` and
`hudi-flink2.2.x:1.3.0-SNAPSHOT`. These are modules in this checkout, not published dependencies to
obtain from Confluent or JitPack. The earlier local root command omitted the engine profile flags.
On this machine, `mvn -N help:active-profiles` showed only `java17` active
(`/tmp/hudi-root-active-profiles.log`). Activating that profile disables the root POM's
`activeByDefault` Spark/Flink profiles. Default properties still name the adapters, but their
profile-contributed modules are absent from the reactor.

Always explicitly select `-Dscala-2.12 -Dspark3.5 -Dflink2.2` for this local CI configuration,
as the workflow does. The corrected root command above includes these flags. No POM or source
change is required. An explicit-profile reactor lists both adapter modules; build/install them
and their required modules with:

```bash
mvn -Dscala-2.12 -Dspark3.5 -Dflink2.2 -DskipTests -DskipITs \
  -pl hudi-spark-datasource/hudi-spark3.5.x,hudi-flink-datasource/hudi-flink2.2.x -am install
```

This is dependency preparation; tests are skipped in this stage and must run in the test stage.

The preparation completed successfully for all 18 required modules in 1 minute 18 seconds
(`/tmp/hudi-local-engine-adapters.log`), installing both missing adapter JARs. The complete explicit-profile
root reactor then passed `validate` across 51 modules (`/tmp/hudi-explicit-engine-reactor.log`).
Validation confirms the build configuration, not test results; the complete root test run remains
unverified. No source, test, or POM changes were made for this dependency-resolution issue.

### Follow-up: ORC's Java 17 Hadoop runtime dependency

The explicit Spark 3.5 profile selects ORC 1.9.1. Its `orc-shims` published POM has an automatically
activated Java 17 profile that contributes `hadoop-client-api:3.3.5`, without the matching runtime.
`hudi-client-common` then loads Hadoop classes from that API JAR, which require shaded classes in
`hadoop-client-runtime:3.3.5`. The failed report's classpath contained the API JAR but no Hadoop
client runtime. Most failures were `ClassNotFoundException` for
`org.apache.hadoop.shaded.com.ctc.wstx.io.InputBootstrapper` during Hadoop initialization, with
subsequent static-initialization errors. The dependency path is recorded in
`/tmp/hudi-client-common-hadoop-tree.log`.

The unchanged `TestHoodieMetadataWriteUtils#testCreateEmptyNativeLogFile` reproduced the failure
(`/tmp/hudi-client-common-hadoop-classpath.log`) and passed after adding the matching runtime via
Surefire's `maven.test.additionalClasspath` launch property
(`/tmp/hudi-client-common-hadoop-runtime-fixed.log`). No source, test, or POM was edited. This
workaround supplies a missing dependency for local tests; it does not change the published POM.

Prepare this JAR on a fresh machine before using the updated local root command above:

```bash
mvn -N dependency:get -Dartifact=org.apache.hadoop:hadoop-client-runtime:3.3.5 -Dtransitive=false
```

The artifact has been downloaded on this machine (`/tmp/hudi-hadoop-runtime-prepare.log`), and its
contents include the missing `InputBootstrapper` class. The property is a Surefire test-classpath
setting; it does not alter production artifacts or replace the existing JVM arguments.

The full `hudi-client-common` module passed with the explicit engine profiles and added runtime:
**1,423 tests, zero failures/errors, 2 skips**, recorded in
`/tmp/hudi-client-common-full-runtime-fixed.log`. This verifies the affected module, not the
entire root reactor with the expanded launch configuration.


### CI follow-up: local stage reproduction and Kafka fixture fix (2026-09-07)

**The overall change remains failing in the Spark serialization stage.** No production files or
existing test assertions were changed during this follow-up. The changes are confined to
`hudi-utilities/src/test/java/org/apache/hudi/utilities/testutils/KafkaTestUtils.java` and the new
`TestKafkaTestUtils.java` regression test.

#### Historical CI comparison and serialization causality

- Earlier green revision `9c6cc679b8`: https://github.com/ravjot28/hudi/actions/runs/33337749962.
  Its Spark Java 17 part 1 job explicitly reports all three `TestHoodieRecordSerialization` cases
  passing. The parent baseline `8e88496082` also has a green CI run, `33315618052`.
- Original push `194b4a006b`: https://github.com/ravjot28/hudi/actions/runs/34009798070.
  The only failed job is Spark Java 17 part 1: serialized size expected 169, actual 822.
- Fork sync `4738d52b26`: https://github.com/ravjot28/hudi/actions/runs/34009820575.
  Same serialization failure, plus the utilities Kafka offset test failure.
- Local Spark build used the `bot.yml` Java 17 / Scala 2.13 / Spark 4.2 build command, including
  the Spark example dependency closure. Build succeeded; `/tmp/hudi-local-ci-spark42-build.log`.
- Local full Java UT 1 stage used the workflow's five modules and exact exclusion filter:
  `-Punit-tests -Pjava17 -Dscala-2.13 -Dspark4.2 -DwildcardSuites=skipScalaTests`
  `-Dtest=!TestCOWDataSource,!TestMORDataSource,!TestHoodieFileSystemViews`
  `-Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=false`.
  Modules: `hudi-common`, `hudi-spark-datasource/hudi-spark`,
  `hudi-spark-datasource/hudi-spark-common`, `hudi-spark-datasource/hudi-spark4-common`,
  `hudi-spark-datasource/hudi-spark4.2.x`. Host timezone was UTC. Host Java is Arch OpenJDK
  17.0.20.1, whereas CI uses Temurin 17; this was not a byte-for-byte OS/JDK image replica.

| Local Spark stage module | Cases | Result |
| --- | ---: | --- |
| hudi-common | 2,495 | Pass |
| hudi-spark-common_2.13 | 140 | Pass |
| hudi-spark4-common | 30 | Pass |
| hudi-spark4.2.x_2.13 | 3 | Pass |
| hudi-spark_2.13 | 1,344 | One failure, 64 skips |

The failing assertion is exactly CI's 169-versus-822 size check, repeated on all four attempts.
Full log: `/tmp/hudi-local-ci-spark42-tests.log` (39m34s).

A controlled JUnit launcher comparison used the unchanged three serialization tests and their
recorded classpath. Only the payload class was replaced, by prepending a temporary directory
containing `BaseAvroPayload` compiled from `9c6cc679b8`. Earlier payload: **3/3 pass**. Current
payload: **2 pass, one fails 169 versus 822**. Temporary sources, runner, classpath and logs are
under `/tmp/hudi-ci-payload-comparison/`. This was a class-isolation experiment, not a claim that
an entire old checkout was built. The repository payload and test sources were untouched.

The added writer-schema JSON in `BaseAvroPayload.write` changes the serialized representation and
adds per-record overhead. Its resolution is still open; do not silently relax the size assertion
or remove the schema preservation contracts added for Java MOR.

#### Kafka fixture race: before/after validation

CI's utilities stage uses the image
`apachehudi/hudi-ci-bundle-validation-base:azure_ci_test_base_java11`, digest
`sha256:b76f29237ba134c603df5d3ea2337b8487db29f1d6211c10dd0ea044f6e531dc`.
The local run used that image directly with the repository bind-mounted, Temurin 11.0.28,
Maven 3.6.3, UTC, and Kafka image `confluentinc/cp-kafka:7.7.1`.

Local adaptations: host networking for sibling test containers, UID/GID 1000 to preserve file
ownership, temporary passwd/group mappings, and Docker socket group 968. Maven cache was mounted
at `/maven-cache`; logs and mappings at `/ci-results` (host path
`/tmp/hudi-local-ci-utilities-artifacts`). Initial UID/socket mapping errors were environmental
setup failures and are excluded from the valid test comparison. No host system configuration
was edited.

The 21-module utilities dependency closure was built with the CI engine profiles and its 8 GiB
build heap. The test command uses a 4 GiB Maven heap:

```bash
mvn test -Punit-tests -Dscala-2.12 -Dspark3.5 -Dflink2.2 -fae \
  '-Dtest=!TestHoodie*' -Dsurefire.failIfNoSpecifiedTests=false \
  -pl hudi-utilities -Pwarn-log -Djacoco.skip=false
```

The actual runs also retained workflow logging/retry options and used
`-Dmaven.repo.local=/maven-cache/repository -Duser.home=/tmp` inside the container.
Recompiling changed utilities tests under the 4 GiB test-stage heap hit a compiler heap error;
compiling first with CI's 8 GiB build heap succeeded. This was a launch/resource correction,
not a source or POM change.

| Validation | Result | Log under `/tmp/hudi-local-ci-utilities-artifacts/` |
| --- | --- | --- |
| Unchanged full utilities stage | 1,170 cases, zero final failures/errors, four skips, **two flakes** | `baseline-stage-user-fixed.log` |
| New readiness regression against unchanged helper | 20 cases, **one failure**: configured retention 86400000, observed stale default 604800000 | `readiness-before.log` |
| Same readiness regression with helper fix | **20/20 pass** | `readiness-after.log` |
| Fixed full utilities stage | **1,190 cases, zero failures/errors, four skips, zero flakes** | `fixed-stage-final.log` |

The fixed full stage enabled `-Dsurefire.failOnFlakeCount=1` and Checkstyle, and completed in
6m32s. Checkstyle reports zero violations. Surefire XML confirms no flaky nodes for the new
20 readiness cases, all 30 Kafka offset cases, all 18 JSON Kafka cases, and all 12 protobuf Kafka
cases. Four skips are existing stage skips, not additions made here.

The helper now waits up to 30 seconds for consumer-visible partitions and leaders, then checks
that configuration reads reflect the requested properties. It retries the transient
unknown-topic response while metadata propagates. The wait fixes setup readiness; the business
assertions and existing failure expectations remain intact. The new regression checks both
explicit and default retention settings repeatedly without retrying its assertions.

Kafka documents the acknowledgement/visibility distinction at
https://kafka.apache.org/28/javadoc/org/apache/kafka/clients/admin/KafkaAdminClient.html.
The helper and Kafka tests were unchanged between the earlier green revision and the MOR push;
the isolated readiness test exercises Kafka setup without invoking MOR payload serialization.
A prior green run therefore does not establish that this fixture race was introduced by MOR.

No CI workflow was rerun or new commit pushed by the agent. The full root reactor and every CI
matrix stage have not been verified locally; only the two requested failing stages were run.

## Original-payload restore comparison — 2026-09-07

The restore experiment uses a `git archive HEAD` checkout and a separate Maven repository under
`/tmp/hudi-payload-restore-6iu0cjla`. No production file or test assertion in the working tree was
changed. The user's staged Kafka readiness fix was preserved. Only `BaseAvroPayload.java` was
substituted between variants; all current tests and the remaining implementation stayed fixed.
This tests the effect of restoring that class, not the entire pre-MOR branch.

Variants:

- `current`: payload from HEAD `4738d52b26f3a6674fe99c08424c1694f0d52ad3`.
- `original`: exact payload from `8e884960820a081e34cdce8e78505e9ed54dff28`, parent of
  the MOR push `194b4a006b3dc07b36808324cf919a0f28d7fdd6`.
- `legacy-kryo-write`: current payload, replacing the negative length and writer-schema JSON
  header with `output.writeInt(bytes.length)`. Schema-aware record decoding, Java serialization,
  and the current Kryo reader are retained. This is a write-format rollback only.

The unchanged Spark serialization test and shared payload tests ran with Java 17, Scala 2.13,
Spark 4.2, UTC, and the isolated dependency build:

| Variant | Spark serialization (3 cases) | Shared payload (10 cases) |
| --- | --- | --- |
| Current | 1 failure: expected 169 bytes, actual 822 | All pass |
| Original | All pass | 2 failures, 7 errors |
| Legacy Kryo write only | All pass | 1 failure |

The current implementation writes writer-schema JSON on every Kryo payload. Substituting only
the old write header removes the Spark failure. This confirms the serialization size regression
is caused by our shared payload change. The remaining shared failure with the narrow rollback is
`getRecordHandlesProjectionSerializationAndEvolutionWithoutDataLoss`, specifically projection
by field name after a Kryo round trip. The old format loses the writer schema needed to interpret
the original bytes when the first requested reader schema is a projection.

The full original class additionally loses correct named projections with omitted metadata,
repeated narrow/full reads, missing-required-field validation, named union evolution, and Java
serialization after projection. Restoring it wholesale therefore discards correctness fixes.

Logs are `logs/{current,original,legacy-kryo-write}-{spark,payload}.log` under the study directory;
archived Surefire reports have matching `-reports` directories. `results.json` records commands
and counts. Final Maven summaries are authoritative: Surefire XML after retries undercounts
cases if its final per-class `tests` attributes are simply summed.

### Full Java-client comparison in the Java 11 CI environment

The dependency build and full Java-client suite use
`apachehudi/hudi-ci-bundle-validation-base:azure_ci_test_base_java11` (Temurin 11.0.28,
Maven 3.6.3), UTC, Scala 2.12, Spark 3.5, and Flink 2.2. Dependencies were built with an 8 GiB
Maven heap; test runs use 4 GiB. The comparison runs all module tests, including functional and
performance tests, with `-Dsurefire.failOnFlakeCount=1`; it does not use `-Punit-tests`.

The initial dependency build exhausted the Checkstyle heap. An incremental build with Checkstyle
and RAT skipped succeeded. Those skips are retained for the experiment; test assertions and
performance budgets are unchanged. This is a test comparison, not a complete style/CI gate.

```bash
mvn -Dmaven.repo.local=/maven-cache -Duser.home=/tmp \
  -Dscala-2.12 -Dspark3.5 -Dflink2.2 -DskipITs -Dgpg.skip -Djacoco.skip \
  -Dcheckstyle.skip=true -Drat.skip=true -Pwarn-log -ntp -B -Duser.timezone=UTC \
  -pl hudi-client/hudi-java-client -Dsurefire.failOnFlakeCount=1 test
```

All three full-module comparisons completed:

| Variant | Cases | Final failures | Errors | Skips | Flakes | Strict gate |
| --- | --- | --- | --- | --- | --- | --- |
| Current | 457 | 0 | 0 | 0 | 1 | Failed |
| Original | 457 | 1 | 0 | 0 | 1 | Failed |
| Legacy Kryo write only | 457 | 0 | 0 | 0 | 1 | Failed |

The current and legacy-write variants pass all 198 Java read-client cases, including the four
measured-workload cases, and all four payload schema-mismatch cases. This does not override the
legacy-write variant's shared Kryo projection failure or establish all-profile performance.

Original alone persistently fails
`TestAvroPayloadSchemaMismatch.testPartialUpdateAvroPayloadAppliesUpdateWithMetaDeclaringSchema`:
the expected updated record is `{id1=id1,Danny,111,2,par1}` but the old record
`{id1=id1,Danny,1,1,par1}` remains on all four attempts. The current control passes this case.
All three variants needed a retry for
`TestJavaHoodieBackedMetadata.testReattemptOfFailedClusteringCommit`, at final metadata file-list
validation. This experiment does not establish that flake's root cause or whether other MOR
changes contribute; restoring the payload alone does not eliminate it.

The first-attempt metadata mismatch occurs in all three variants: the metadata listing and actual
filesystem contain different file IDs for the same clustering commit in partition `2015/03/16`.
The test file is identical to the pre-MOR parent. Neither fact establishes whether some other
implementation change contributes; a full parent-versus-current comparison is still needed to
attribute this clustering failure. The payload experiment must not be described as that comparison.

Logs: `logs/java11-{current,original,legacy-kryo-write}.log`, with archived reports and
`java11-results.json`. The runner is `run-java11-matrix.py` in the same study directory.

### Profile and coverage limitations

An initial full Java-client comparison used the Spark 4.2 dependency profile. Each variant ran
438 cases. All three encountered the same ten Hadoop/Avro errors (including
`THREAD_INHERITS_SUBJECT` and `LocalDate`/integer mismatches) and one measured-workload allocation
budget failure (about 8.89–8.93 GB allocated against the existing 8 GiB budget). Original also
failed the partial-update case. These shared failures did not reproduce in the current Java 11 /
Spark 3.5 control; they cannot be attributed to the payload rollback from this evidence. They
remain unaddressed findings for the Spark 4.2 dependency profile. No performance budget was raised.

Java-client CI's unit job excludes functional tests, but a separate `FT - common and other
modules` step includes them. In the failed fork-sync run, the preceding utilities step failed,
preventing that functional step from running. A green Java-client unit job alone does not prove
the full MOR functional suite passed; the workflow does contain that coverage.


## 2026-09-07: scoped compatibility fix and renewed MOR parity audit

The user reaffirmed the original handoff scope: implement standalone Java MOR read/write and port
applicable storage contracts from the other engines. No existing assertion was relaxed, disabled or
converted to an expected failure. Spark's unchanged record serialization test still requires 169 bytes.

### Serialization fix

`BaseAvroPayload.useLegacyKryoFormat` selects the original payload encoding on one Kryo instance.
`HoodieSparkKryoRegistrar` selects it because Spark supplies the writer schema to payload consumers.
Unregistered/standalone Kryo instances continue carrying the writer schema, preserving projection and
schema evolution after serialization. The reader accepts both formats. The mode survives graph resets;
registration order and IDs are unchanged. The Kryo selection does not change Java serialization behavior. The serialization identity fix described below separately preserves compatibility with pre-MOR Java streams.

`TestHoodieSparkKryoRegistrar` now checks live records and tombstones through independent registered
writer/reader instances over three graph resets. It checks the legacy header, ordering value and field
values/projection. `TestBaseAvroPayload` checks that configuring one legacy writer cannot change a fresh
standalone writer's self-contained format; its projection test also uses an independent deserializer.

Focused validation, Java 17 / Spark 4.2 / Scala 2.13, with style checks enabled:

| Suite | Cases | Failures/errors/skips/flakes |
| --- | ---: | --- |
| `TestBaseAvroPayload` | 11 | 0/0/0/0 |
| `TestHoodieSparkKryoRegistrar` | 3 | 0/0/0/0 |
| unchanged `TestHoodieRecordSerialization` | 3 | 0/0/0/0 |
| unchanged `TestAutoKeyGenForSQL` | 2 | 0/0/0/0 |

Log: `/tmp/hudi-pr-readiness/logs/mor-serialization-focused-final.log`.
The earlier `serialization-focused.log` used a stale compiled common test class (10 cases); it is not
counted as validation of the new eleventh test. The final focused run above recompiled and ran all 11.

### Additional Java acceptance coverage

All additions are in `TestHoodieJavaReadClient`. This is 22 additional invocations, increasing the
expected full Java module count from 457 to 479. This arithmetic is not a substitute for the final
combined Maven result below.

| Java test | Source contract | Variants and assertions |
| --- | --- | --- |
| `testCdcEnabledMorWritesAreConsumableByCommonCdcExtractor` | Flink `TestWriteMergeOnRead.testInsertDuplicateRecordsWithCDCMode` and CDC-enabled direct-client writes | Six instead of one: OP_KEY_ONLY / DATA_BEFORE / DATA_BEFORE_AFTER × log-only / prior compaction. Check one visible repeated key, exact updated row, empty snapshot after delete, all five insert/duplicate/update/delete/reinsert commits in common CDC splits with source paths, and exact reinserted value after compaction. This validates write and split-planning interoperability, not a Java CDC row-output API. |
| `testComplexValuesSurviveMorCompactionAndClustering` | Spark `TestHoodieSparkMergeOnReadTableCompaction.testDecimalFixedWidthPreservedAfterCompactionAndClustering`, `TestMORDataSource.testLogicalTypesReadRepair`, shared/Hive complex schema reads | Six: versions 8/9/10 × log-only / base-plus-log. Fixed(10) decimal precision/scale and positive/negative values; nested nullable Unicode values; empty/nonempty arrays, maps and bytes; signed date values and microsecond timestamps. Compare exact data and multiplicity through snapshot, incremental, compaction, clustering and subsequent write/compaction. Assert the log-only and clustering preconditions actually occurred. Normalize Avro physical versus logical date/time representations without dropping precision. |
| `testMultipleOrderingFieldsAcrossCompaction` | Spark `TestMORDataSource.testMultipleOrderingFields` | Six: versions 8/9/10 × log-only merging / compaction after every write. Equal primary field uses secondary ordering; larger primary wins despite smaller secondary; older primary loses despite larger secondary. Exact winner and single-row multiplicity at every step. |
| `testMorWriteReadLifecycleWithEachJavaIndex` | Java index factory and Spark index-aware MOR write/compaction contracts | Five: SIMPLE, GLOBAL_SIMPLE, INMEMORY, BLOOM, SIMPLE BUCKET. Insert two partitions, update one key, compact, delete, reinsert and compact again. Verify exact untouched and changed values and multiplicity. The fixture explicitly uses EngineType.JAVA and recomputes the layout for the selected index. |

Focused logs: `mor-parity-additions-final.log` (12 pass), `mor-multiple-ordering.log` (6 pass),
`mor-index-lifecycle-final-java-engine.log` (5 pass), under `/tmp/hudi-pr-readiness/logs/`.
The final Java 11 run also validates the explicit Java engine setting added to the two versioned fixtures.
Earlier new-fixture runs exposed an immutable delete input, raw-vs-logical Avro comparison, and inherited
index-layout/engine defaults. These fixtures were corrected; they are not evidence of product defects.

### Engine-neutral coverage map rechecked against repository tests

This maps contracts rather than claiming literal equality with every engine's test count. The detailed
parameter matrices and fault/performance assertions earlier in this document remain applicable.

| Contract and engine references | Java coverage |
| --- | --- |
| Spark `TestHoodieClientOnMergeOnReadStorage`: base-less reads, regular/log compaction, scheduling order, cleaner with pending compaction | `TestHoodieJavaClientOnMergeOnReadStorage` corresponding methods; snapshot/optimized read cases |
| Spark MOR insert/update/delete tests and Flink base/log, deletes, disordered update/delete input-format tests | Write operation lifecycle including prepared entry points, prepped delete, empty writes, duplicate input, both merge modes, delete/reinsert and partition-scoped keys |
| Spark log-compaction rollback/archival | `testLogCompactionRollbackPreservesSnapshot` (completed/inflight × file shape), `testLogCompactionArchivalPreservesSnapshot` |
| Spark MOR rollback/restore and pre/post-MDT failure tests | Marker/listing rollback, savepoint across compaction/inflight write, commit failure phases, rollback file-delete and MDT storage failures; exact snapshot and subsequent write checks |
| Spark and Flink NBCC partial-update/inflight/multiple-base-file and six bulk-insert conflict sequences | `TestJavaNonBlockingConcurrencyControl`, plus out-of-order completion, concurrent delete/upsert, abandoned compaction recovery, bucket routing/defaults and partition-bucket rejection |
| Concurrent writers outside NBCC | `testOptimisticConcurrentWritersPreserveCommittedRows`: overlapping/disjoint file groups × both file shapes, conflict visibility/rollback and retry |
| Spark inflight compaction/read-optimized and Flink pending compaction input formats | `testReadsIgnoreInflightCompactionBaseFileButSeeLaterDeltaCommit`, `testSnapshotIncludesRecordsWrittenAfterCompactionScheduled`, time-travel rejection of pending compaction |
| Spark/Flink completion-time incremental, hollow instants and skip-compaction/clustering | Bounded/earliest/null-start and late-writer cases; compaction start boundaries, touched partitions, deleted rows, overwrite/clustering filtering and repeated file-group writes |
| Spark clustering, cleaner and savepoint storage invariants | Clustering exact rows and incremental boundaries, all cleaner policies with actual deletion checks, savepoint/restore, partition/table overwrite, TTL and partition delete |
| Spark/Flink schema evolution and shared Hive readers | Internal-schema rename, added defaults, legacy aliases/positional compatibility, metadata-free snapshot/optimized reads, partial-update regressions, Java and Hive file-group-reader concrete suites; new complex-value cases |
| Spark spill and Flink small log blocks | Both spill stores and close paths, one open merge buffer over many file groups, actual log rollover, shared iterator depth/failure handling and storage read failures |
| Storage compatibility/recovery beyond a single engine fixture | Base/log format pairs at v6/v8/v9, native v10 logs, codecs inspected in real Parquet footers, directed version transitions, commit faults, abrupt subprocess death and shared log corruption/HDFS append tests |
| Performance behavior | Four measured skew/wide/history workloads with time/allocation budgets, exact scan/compaction results, real spill, bounded simultaneously open readers, and previously recorded scale-16 run. These are regression workloads, not production throughput or latency certification. |

Scope boundaries remain explicit: no Java SQL/vectorized/Catalyst equivalents, Flink checkpoint/operator
or RowData source plumbing, Java CDC row-output API, archived incremental fallback, LSM reader,
metadata-free incremental filter, custom full-bootstrap provider, or consistent-hashing bucket engine.
The factory's supported SIMPLE bucket engine is covered. Full Spark/Flink feature parity must not be
claimed for these exclusions. The tests do not exhaust all combinations of every axis or distributed
production failure pattern.

### Baseline flake and rejected scope expansion

A full pre-MOR source archive at `8e884960820a081e34cdce8e78505e9ed54dff28` reproduced
`TestJavaHoodieBackedMetadata.testReattemptOfFailedClusteringCommit` as a retry-recovered failure in
`parent-reader-metadata.log` (116 cases). The same sequence on the scoped current code passed all 116
in `current-reader-metadata.log`. Each metadata class alone passed 68 (`parent-metadata.log`,
`current-metadata.log`). This is stronger evidence than substituting only BaseAvroPayload.

The failure compares stale metadata file IDs against newly created file IDs. Separate deterministic
experiments showed cached HFile blocks surviving deletion/recreation at the same path. The experimental
HFile fix and its regression test were removed from the working tree on the user's scope correction;
only `/tmp/hudi-pr-readiness/deferred-hfile/` retains them. The fix is not fully validated and is not
included. Existing metadata assertions remain unchanged. A later green run cannot establish the flake
has been eliminated.

### Validation environment corrections

- A Spark 4.2 incremental build reused a Spark 3.5 generated ANTLR parser, producing ATN-version errors.
  A clean profile-specific build resolved this local artifact problem. That invalid run is not a source
  regression result.
- The Java 11 combined build ran out of heap during Checkstyle. It was rerun with CI's 8-GiB Maven and
  compiler settings and CI's separate style/test phase settings (`checkstyle.skip`/`rat.skip` in the test
  runner). Scoped style checks passed in the host Maven runs. No repository build settings were edited.
- Java 17 Spark tests require opening `java.base/java.util.concurrent.locks` for Kryo. This is supplied
  in the local runner environment; no existing test was edited for it.
- Runners use `-Dsurefire.failOnFlakeCount=1`, UTC, and an ephemeral ZooKeeper admin port. Passing after a
  retry is therefore reported as a failed stage, not silently called green.

Final stage results are recorded in the final-source and closing tables below. Runner commands and JSON results are in `/tmp/hudi-pr-readiness/`.
The initial Java 11 full module run with the scoped serialization fix completed **457 tests, zero
failures/errors/skips/flakes** in 11m20s (`mor-java11-full.log`). This run predates the 22 added invocations.
Final explicit `checkstyle:check` passed with zero violations in common, Spark-client and Java-client
(`mor-final-checkstyle.log`). `validate` alone was only a Maven validation phase, not a Checkstyle run.


For a Java-only reproduction from a fresh checkout, use Java 11 (check `mvn -v`), UTC and the same
Spark/Scala dependency profile in both phases. Install upstream reactor artifacts before the test phase;
`test` alone does not produce all snapshot dependency artifacts needed by downstream modules.

```bash
TZ=UTC MAVEN_OPTS=-Xmx8g mvn -Dscala-2.12 -Dspark3.5 -Dflink2.2 \
  -pl hudi-client/hudi-java-client -am -DskipTests -DskipITs \
  -Dmaven.compiler.maxmem=8192m install
TZ=UTC MAVEN_OPTS=-Xmx4g mvn -Dscala-2.12 -Dspark3.5 -Dflink2.2 \
  -pl hudi-client/hudi-java-client -DskipITs -Duser.timezone=UTC \
  -Dzookeeper.admin.serverPort=0 -Dsurefire.failOnFlakeCount=1 \
  -Djacoco.skip=false test
```

Do not add `-Punit-tests` to the second command: the MOR functional and measured-workload cases must run.
The actual validation below uses the repository CI Java 11 container image and an isolated checkout/repo;
its complete Docker/Maven command arrays are retained in the results JSON.


### Final review found and fixed Java serialization identity drift

The schema-retention change added a static initializer to `BaseAvroPayload`, changing its generated
Java serialization ID. Adding the Kryo transport selector changed it again. Direct compilation and
`ObjectStreamClass.lookup` comparison established these values:

| Source | Generated serialVersionUID |
| --- | ---: |
| Pre-MOR parent `8e884960820a081e34cdce8e78505e9ed54dff28` | 4076216714695518773 |
| HEAD before this continuation | 5491238539750785804 |
| Scoped transport selector before pinning | 8544408039941323894 |

This prevented the legacy-read fallback from ever receiving old Java-serialized payloads: Java first
threw `InvalidClassException`. This is a compatibility issue caused by our changes, within the requested
scope. The class now explicitly retains **4076216714695518773L**.

Two golden byte streams were produced using the pre-MOR class and the unchanged concrete payload class:
one materialized live record and one tombstone. The new parameterized
`TestBaseAvroPayload.javaSerializationReadsPreMorPayloads` verifies both, ordering value, supplied writer
schema, reordered projection and a later full-schema read. Both cases failed on every attempt before
pinning; after pinning, all **13** payload cases pass under both Java 17 and Java 11. The old serializer
required materializing `recordBytes` before serialization; the fixture states this precondition.

Evidence: `/tmp/hudi-pr-readiness/uid-probe/`, `logs/mor-java-serialization-before.log`,
`logs/mor-java-serialization-fixed.log`, and `logs/mor-final-payload-java11.log`.
The last log includes the Maven install used by the isolated final Java-module run.

The first expanded Java module run passed **479** cases with no failures/errors/skips/flakes
(`mor-java11-parity-full.log`), and the selected unchanged Hive schema/complex-value methods passed
**17** (`mor-java11-hive-schema.log`). These runs preceded the serialization-ID pin. A fresh isolated
source and Maven-repository snapshot now reruns Java against the final source including the pin;
its results are kept separately in `final-snapshot-results.json`. Final Spark 3.5 serialization checks
use the same final artifacts and are recorded in `final-spark-compat-results.json`.


### Exact final-source Java validation

The five changed production/test files match the isolated validation checkout byte-for-byte;
SHA-256 values are saved in `/tmp/hudi-pr-readiness/final-source-sha256.json`.
No deferred HFile changes are present. The user's staged Kafka fixture changes are preserved.

| Final-source stage | Tests | Failures | Errors | Skips | Flakes | Time | Log in `/tmp/hudi-pr-readiness/logs/` |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| Java 11 payload compatibility | 13 | 0 | 0 | 0 | 0 | 15s | `mor-final-payload-java11.log` |
| Full Java-client module, including functional and performance cases | 479 | 0 | 0 | 0 | 0 | 12m28s | `mor-final-java11-full.log` |
| Selected unchanged Hive schema/complex-record methods | 17 | 0 | 0 | 0 | 0 | 9s | `mor-final-hive-schema.log` |
| Spark 3.5 registrar + unchanged record serialization | 6 | 0 | 0 | 0 | 0 | 55s | `mor-final-spark35-serialization.log` |

The expanded read-client class accounts for **220** cases within the 479. All acceptance assertions
remain active. Shared unit stages on the scoped transport fix before the final ID pin also passed:
IO 126, common 2,495, Hadoop common 1,279 (3 skips), client common 1,423 (2 skips), Spark-client
933 (15 skips): **6,256 total, zero failures/errors/flakes, 20 existing skips**.
Spark-client's functional stage passed **46** with zero failures/errors/skips/flakes.
These are separate profile runs, not a count of distinct cross-profile tests.

The final Java measured-workload reports are archived in `logs/mor-final-performance/`:

| Keys | Value width | Skewed | Write ms | Six snapshot scans ms | Compaction ms | Main-thread allocation bytes |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 256 | 16 | false | 2,128 | 6,732 | 262 | 5,750,961,808 |
| 256 | 4,096 | true | 1,148 | 3,492 | 173 | 3,037,467,280 |
| 2,048 | 16 | true | 1,463 | 3,341 | 188 | 3,164,932,144 |
| 2,048 | 4,096 | false | 2,733 | 7,567 | 353 | 6,636,502,208 |

These runs use Java 11 and JaCoCo while other isolated validation stages run on the host. The unchanged
per-case guards are 180 seconds and 8 GiB main-thread allocation. Measurements exclude allocations on
other threads and are not production SLAs or matched Spark/Flink throughput benchmarks.
The earlier scale-16 evidence remains a separate, differently instrumented historical run.

A reverse-compatibility probe also wrote live/tombstone Java streams with the final class and read them
using the pre-MOR class, forcing field decoding and checking ordering/data; both passed.
See `uid-probe/reverse-compatibility.log`. This probe is additional evidence, not another JUnit case count.

### Broader Spark validation findings kept separate

The Java 11 utilities stage initially reused a generated Spark 4 parser with Spark 3.5's ANTLR runtime,
causing `Could not deserialize ATN with version 4 (expected 3)`. A clean build of `hudi-spark-common`
alone did not fix it: the generated `HoodieSqlCommonLexer` belongs to **hudi-spark**. Cleaning/installing
that module under Spark 3.5 fixed the six focused transformer checks. The full utilities stage was rerun successfully from the isolated final snapshot. No parser source or existing test was changed.

The broader host Spark 4.2 run encountered two class-loading errors after a common-module test build
replaced the test JAR while Spark was already using it. `hudi-common/pom.xml` binds `test-jar` to
`test-compile`, and Spark's classpath uses that JAR. Both allegedly missing classes exist in the rebuilt
JAR (`TestInternalSchemaConverter`, `HoodieBackedTestDelayedTableMetadata`). This overlapping validation
was an agent-side orchestration error; its results cannot establish a source regression. Follow-up checks used fixed build outputs after the broader run completed; all affected cases passed.

It also encountered `TestIncrementalQueries.testIncrementalQueryWithMultiCommitsInSameFile` producing
`20260907211499816` by subtracting an integer from a formatted timestamp near a minute boundary.
The test's arithmetic and `HoodieSqlCommonUtils.validateInstant` are unchanged from pre-MOR parent
8e884960820a081e34cdce8e78505e9ed54dff28. Subtracting 200 from `20260907211500016` yields the invalid
seconds value 99; this is not time-aware subtraction. The existing test is left unchanged per scope.
A later pass does not eliminate this pre-existing timing defect.


### Closing validation results

| Stage | Result | Evidence |
| --- | --- | --- |
| Corrected Java 11 utilities CI selection | **1,190 tests, zero failures/errors/flakes, four existing skips**, 7m57s | `logs/mor-final-utilities.log`, `final-utilities-results.json` |
| Final Spark 4.2 follow-up with stable artifacts | **29 tests, zero failures/errors/skips/flakes**, 1m47s | `logs/mor-final-spark42-followup.log`, `final-spark42-followup-results.json` |
| Broad Spark 4.2 stage before artifact isolation | 4,013 tests across five modules; **two errors, one flake, 64 skips**; failed strict gate | `logs/mor-spark42-ci-final.log`, `spark-results.json` |

The 29 follow-up cases comprise 13 payload tests, three registrar tests, three unchanged record
serialization tests, six incremental-query cases, one shared-schema converter case and three remote
filesystem-view cases. Both prior missing-class failures pass with the JAR held fixed. All original
failure reports were preserved under `logs/spark42-original-failure-reports/`.

The broad Spark stage's two errors came from missing shared test classes after a concurrent common test
JAR rebuild. Its remaining retry-recovered failure came from the unchanged timestamp-arithmetic test.
The targeted follow-up is not a claim that the whole stage was rerun green, nor that the timestamp flake
was repaired. No existing unrelated Spark test was modified, and no tests were excluded to hide failures.
No remote CI run of these unstaged changes has been performed.

Within the original handoff scope, the final standalone Java MOR suite and its additional acceptance
cases pass under the Java 11 CI profile. The existing limitations listed above remain explicit, and no
claim is made of literal all-feature parity, exhaustive fault coverage, production performance SLA,
or a wholly green repository-wide CI matrix. All validation processes launched for this continuation
have completed. The user's staged Kafka changes remain unchanged; production changes in this
continuation are confined to BaseAvroPayload and the Spark Kryo registrar.
