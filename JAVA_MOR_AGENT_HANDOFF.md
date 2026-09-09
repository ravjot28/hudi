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

# Java standalone MoR work: agent handoff

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


Last updated: 2026-09-08

## RecordMerger assessment — 2026-09-08

The requested assessment is recorded in
`target/java-mor-merger-assessment/ASSESSMENT.md`, with the isolated checkout, Maven repository,
temporary test probe, result manifest, and logs alongside it. With the four payload-PR files
restored to Apache parent `a7deb61f7426`, 15 native Avro cases passed across event-time,
commit-time, and custom RecordMerger modes (three mode-inapplicable skips). The custom probe
asserts its persisted strategy and actual invocation during snapshot reading and compaction;
built-in partial updates preserve an untouched field across compaction. The existing legacy
PartialUpdateAvroPayload schema regression still fails consistently (3/4 regression cases pass).

Recommendation: follow Vinoth's RecordMerger direction, make native inputs and modern merge
modes the primary Java path, and assess #19872 as a separate legacy compatibility fix rather
than an unconditional prerequisite. Do not simply remove the fix while promising legacy parity.
Permanent Java custom-merger integration coverage is missing; shared FGR tests do not close
that gap. The assessment lists follow-up coverage, partialMerge limitations, and PR scope work.
No tracked source/test files, branch refs, or public PR/discussion content changed during this
assessment. Only the final logs named `final-native-*` count as verified mode-specific evidence.

## Discussion progress and new design feedback — 2026-09-08

Posted the user-authorized discussion update at https://github.com/apache/hudi/discussions/19798#discussioncomment-18359759.
It links payload PR #19872, lists the remaining local Java/docs/Kafka PRs, thanks Shiyan Xu and
Nadine Farah, corrects the original scope claims, and describes future Java/streaming contributions.
Vinoth Chandar has asked whether the work can use Hudi 1.x RecordMerger APIs; see discussion comment
18357258. The update acknowledges this. Assess that design feedback before finalizing the remaining
Java PRs; using HoodieFileGroupReader does not itself resolve the question about BaseAvroPayload.
Only #19872 is open upstream. Other branches remain local.

## First upstream PR submitted — 2026-09-08

The user authorized moving ahead with PR submission after ASF acknowledged the ICLA.
Pushed `pr/java-mor-payload` and opened [apache/hudi#19872](https://github.com/apache/hudi/pull/19872)
against Apache master, with head `a8530e348f7f27bcbd867bb607253bd1663a9f49`.
GitHub confirms exactly four payload/Spark files, OPEN, non-draft, and mergeable. No checks
had appeared at verification. The body discloses the Kryo compatibility boundary and distinguishes
16 passing split-branch focused tests from the combined fork CI. Other prepared branches remain
local. No issue or discussion comment was posted. Submit later dependent PRs in order, avoiding
overlapping upstream diffs. The new reader API still needs maintainer resolution of RFC treatment.

## Contribution preparation — 2026-09-07

The consolidated commit `e701bebf6cb5c2b62cc79da069b7c3d360097811` was pushed by the user.
Its fork Java CI run [34168606289](https://github.com/ravjot28/hudi/actions/runs/34168606289)
completed successfully: all 17 enabled jobs passed. The Trino workflow also reports success.
This supersedes the earlier absence of a verified remote result, but does not mean every possible
version combination ran: the repository matrix is trimmed.

The user requested PR preparation following discussion #19798, the dev-list email, and Shiyan Xu's
encouragement to open PRs. Four local review branches were created without changing `master`:
`pr/java-mor-payload`, `pr/java-mor-bucket-index`, `pr/java-mor-read-write`, and the independent
`pr/kafka-test-readiness`. The three MOR patches plus the Kafka patch reproduce the original tree exactly.
Their manifest, PR bodies, feature issue, design draft, discussion reply, and website checkout are in
`/tmp/hudi-contribution-prep/`. Its README records requirements, evidence, and remaining actions.

The payload branch builds and passes 13 common payload plus three Spark registrar tests. The bucket
branch builds and passes its two focused partitioner cases. The fresh split MOR run completed 479 cases
with zero final failures/errors/skips, but the unchanged metadata clustering reattempt test passed only
on retry, so `surefire.failOnFlakeCount=1` failed. This matches the earlier complete pre-MOR baseline
reproduction; no assertions or retry gates were changed. All 220 reader cases passed without retries.
Four affected production modules passed separate Checkstyle checks, and PR template/title plus added
source-header checks passed. Do not describe the fresh split suite as strictly green.

The user supplied the ASF Secretary acknowledgement on 2026-09-08: the ICLA is filed in ASF records
and the Hudi PMC has been notified. The ICLA requirement is complete. No ICLA
or personal contact details are included in the review packet. The public API/RFC decision remains with
maintainers: Shiyan's invitation to raise PRs is not an RFC waiver. A companion `asf-site` documentation
change is committed as `d3f8d858e4` on `pr/java-mor-docs` in the isolated website checkout. Its final
site build succeeds, its example compiles on Java 11, and 25 guide links/anchors plus the reading/writing
entry-point links pass. Publication of the split branches, feature
issue, RFC, docs PR, and discussion reply has not been performed by the agent.

The existing discussion/email overstate the absence of Java MOR writing/compaction and constant memory.
The prepared reply corrects those points, updates internal-schema/CDC scope, thanks Shiyan Xu and Nadine
Farah, and describes future Java/streaming work for teams avoiding the operational cost of a Spark deployment.

## Current scope and continuation — 2026-09-07

The user reaffirmed the original handoff scope: standalone Java MOR read/write and applicable
Spark/Flink storage-contract parity. Production fixes needed by those contracts are authorized.
The earlier tests-only instructions and open serialization status below are historical.

Current scoped changes:

- Preserve schema-bearing payload serialization for standalone Java while explicitly selecting the
  original compact payload wire format in Spark's Kryo registrar. This repairs the regression introduced
  by our writer-schema serialization; Spark's unchanged 169-byte assertion remains unchanged.
- Add independent-Kryo, graph-reset, tombstone, and projection compatibility checks. Preserve the
  pre-MOR Java serialization ID explicitly; two golden-stream tests caught the identity drift caused
  by our schema-retention changes. All 13 shared payload tests now pass on Java 11 and Java 17;
  three registrar and three unchanged Spark record serialization cases also pass.
- Expand Java CDC write/extractor coverage from one case to six: all supplemental logging modes crossed
  with log-only/base-plus-log setup, including repeated inserts, update, delete, reinsert and compaction.
- Add six complex-value lifecycle cases (versions 8/9/10 × file shape), covering fixed-width decimal,
  nested nullable values, Unicode, empty/nonempty arrays/maps/bytes, dates and microsecond timestamps,
  through snapshot, incremental read, compaction, clustering and later writes.
- Add six multiple-ordering-field cases across versions 8/9/10 and compaction boundaries.
- Add five MOR write/read/delete/reinsert lifecycles, one for each built-in Java index.

The exact final-source Java 11 suite passed **479** cases with no failures/errors/skips/flakes, including
all **220** read-client cases and all four measured workload guards. The 13 final payload tests, 17 selected
Hive schema cases and six Spark 3.5 serialization cases also pass. Shared unit stages passed 6,256 cases
(20 existing skips) and Spark-client functional tests passed 46, before the final Java serialization ID pin.
See the latest evidence section for exact commands, profiles, source hashes and performance measurements.
The corrected utilities stage passed 1,190 cases (four existing skips), and the final Spark 4.2 follow-up
passed 29 with no failures/errors/skips/flakes. The broad Spark 4.2 stage itself was not green: two errors
came from a shared test JAR rebuilt during its run, and one flake from an unchanged timestamp-arithmetic
test. The affected cases passed with fixed artifacts; the timestamp flake remains. Do not claim a green
whole-repository CI matrix or a new remote CI result. All local validation processes have completed.

An unrelated metadata clustering flake was reproduced on the complete pre-MOR parent
`8e884960820a081e34cdce8e78505e9ed54dff28`, using the reader/metadata sequence (116 cases).
An HFile cache experiment was removed from the working tree after the user requested scope discipline.
It remains only under `/tmp/hudi-pr-readiness/deferred-hfile/`; it is not part of this change and its fix
was not fully validated. Do not weaken the metadata assertion or attribute that baseline flake to MOR.
The user's previously staged Kafka fixture change is preserved.

The parity policy below still applies: Java has no Spark SQL/Flink operator API, standalone CDC-output
API, archived incremental fallback or LSM reader. These limitations must remain visible; engine-neutral
parity within the handoff scope is not literal feature parity with every engine feature.

## Historical restore comparison: neither rollback preserves all contracts

The user requested an original-code comparison before choosing a fix. All payload substitutions
were made in `/tmp/hudi-payload-restore-6iu0cjla/checkout`, with an isolated Maven repository.
The working-tree production code and the user's staged Kafka fixture fix were preserved.

The original `BaseAvroPayload` comes from `8e884960820a081e34cdce8e78505e9ed54dff28`,
the parent of the MOR push. With all current tests unchanged, it passes all three Spark
serialization cases but fails nine of ten shared payload cases. It also silently drops the
partial update checked by `TestAvroPayloadSchemaMismatch`, reproduced under Java 11 / Spark 3.5.

A narrower experimental rollback retains the current schema-aware decoding and Java serialization,
but emits the legacy Kryo payload format. It passes all three Spark cases and nine shared payload
cases; projection by field name after a Kryo round trip still fails. Current code passes all ten
shared cases but fails Spark's unchanged 169-byte assertion with 822 bytes. This isolates the Spark
regression to the added writer-schema JSON in `BaseAvroPayload.write`.

Do not adopt either rollback wholesale, remove the projection contract, or increase Spark's expected
size to hide the regression. Schema correctness and compact Spark serialization still need a joint
solution. The Java 11 full-module comparison is complete: each variant ran 457 cases. Current and
legacy-write variants had zero final failures/errors; original had one persistent partial-update
failure. All three needed a retry for the same metadata clustering test, so all failed the strict
no-flake gate. Its logs show differing file IDs for the same commit between metadata and filesystem.
This class-only comparison does not establish that clustering issue's cause. Full results and
profile limitations are recorded in `JAVA_MOR_TEST_EVIDENCE.md`.

## Historical CI follow-up before the scoped serialization fix

Do not describe the full change as CI-green or production-ready. The original push `194b4a006b`
and subsequent fork sync `4738d52b26` both fail the unchanged Spark serialization test. The
Spark 4.2 / Scala 2.13 / Java 17 CI test stage was reproduced locally: 4,012 cases across five
modules, one persistent failure and 64 skips. `TestHoodieRecordSerialization.testAvroRecords`
expects 169 serialized bytes but receives 822 on all four attempts.

Causality was checked with the same compiled tests and dependencies, substituting only an isolated
`BaseAvroPayload` compiled from the earlier green revision `9c6cc679b8`: all three serialization
tests pass with that class; the current class fails the size assertion. The current writer-schema
JSON serialization is the cause. No production serialization fix was made in this follow-up;
preserving the schema-evolution contracts and addressing serialization overhead remain required.

The utilities Kafka test helper also had an independently reproduced readiness race. Topic/config
acknowledgements preceded consumer-visible metadata/configuration. A new 20-case regression caught
stale retention configuration before the fix. `KafkaTestUtils.createTopic` now waits, with a bound,
for consumer-visible partitions/leaders and the requested configuration. Existing assertions were
kept unchanged. Only the helper and its new regression test were changed in this follow-up.

The complete utilities non-TestHoodie CI stage then passed in CI's Java 11 image: **1,190 tests,
zero failures/errors, four skips, zero flakes**, with `-Dsurefire.failOnFlakeCount=1` and Checkstyle
enabled. The unchanged baseline stage needed retries to recover two missing-topic errors.

Logs: `/tmp/hudi-local-ci-spark42-tests.log`, `/tmp/hudi-ci-payload-comparison/{old,current}.log`,
and `/tmp/hudi-local-ci-utilities-artifacts/`. See the latest section of `JAVA_MOR_TEST_EVIDENCE.md`
for commands, environment details, and exact limitations.

## Active implementation phase — 2026-09-05

The user has now authorized production changes to make the acceptance suite pass and assess Java
client write/read correctness, performance, and production readiness. The test-only phase below is
historical. Keep its positive acceptance contracts enabled; do not turn implementation gaps into skips.

## Objective

Prepare a credible Apache Hudi contribution around Merge-on-Read support for the standalone Java
client. The current branch adds `HoodieJavaReadClient`, Java SIMPLE bucket-index support, Java/NBCC
coverage, and changes in the shared Avro/compaction paths.

The earlier follow-up phase was deliberately limited to tests and investigation. No production source was
changed during that phase. The new tests are intended to distinguish working behavior from unsupported,
incorrect, or incompletely validated behavior before the PR description is finalized.

## Implementation progress (supersedes historical findings below)

The enabled acceptance contracts now have implementations for prepared MOR routing and inflight
transitions, log compaction and pending-log-compaction rollback, partition replacement without
small-file reuse, partition deletion/TTL, and full/metadata-only bootstrap plus bootstrap rollback.
Java reads validate unsupported storage/no-meta incremental configurations and completed time-travel
targets, reconcile internal schemas, and use the full current snapshot for unbounded earliest reads.
Shared fixes retain the true Avro writer schema across projections and Kryo/Java serialization,
resolve aliases/defaults/nested renames, distinguish inline Parquet from native-format logs, preserve
failed append statuses, and close lazy iterators safely on exceptional paths.

Two fixtures needed correction once the missing code executed: bootstrap must configure `id`/`part`
rather than the harness's `_row_key`/`partition_path`; a rollback whose heartbeat deletion failed must
wait for lease expiry before another metadata writer takes over. The metadata configuration now
inherits the data writer's heartbeat settings. Bootstrap coverage additionally checks subsequent
updates, compaction, rollback, and external-source preservation. TTL must auto-commit, as required
by the inline table-service caller and the Spark executor contract.

Verified: 187 distinct common acceptance/payload/reader cases pass; shared write/heartbeat/rollback
regressions 65 pass; Hadoop/HDFS storage/fault contracts 163 pass. The first complete Java run passed
455 cases. Correcting INSERT semantics then exposed six log-only
file-group packing failures in Java/Hive reader contracts. A Java MOR partitioner has now been ported
from Spark to account for log-only slices and pending compaction. All 274 targeted cases passed, then
the final full Java-client run passed **457 cases with zero failures, errors, or skips**, including the
scale-16 performance workloads, in 9 minutes 38 seconds (`2026-09-05T20:12:56-04:00`). The combined
Java and selected shared suites cover **872 distinct passing cases**; overlapping reruns are not added
to this count. No acceptance case has been disabled or changed to expect a missing implementation.

The largest measured workload used 32,768 keys, eight batches (262,144 row versions), 4,096-byte
values, and the default 4-GiB test heap. See the current implementation section of
`JAVA_MOR_TEST_EVIDENCE.md` for final logs, measurements, and reproduction commands. Validation is
Java 17 with local storage and separate embedded-HDFS fault tests; production storage/SLA and
sustained-load qualification remain unestablished. Archived incremental ranges, LSM-tree reads,
custom full-bootstrap providers, and mixed-version use of the new Kryo payload representation have
the limitations documented there. The current working tree includes implementation and test changes.

## Root-build follow-up — 2026-09-06

The user requests that unrelated existing tests remain unchanged and that environment differences
be addressed through the local launch configuration. A proposed timezone-only change to
`TestHoodieActiveTimeline` was removed; that file is identical to HEAD. Running with UTC satisfies
its original expectation. The affected Hadoop common module completed 1,272 tests with zero
failures/errors and 3 skips using the CI unit-test/engine profiles on the available JDK 17.

The subsequent root run exposed Spark failures outside the original selected suites. Kryo required
`--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED` on Java 17, and embedded ZooKeeper
needed `-Dzookeeper.admin.serverPort=0` because port 8080 was occupied. Four existing Spark rollback
assertions also exposed a real regression in our `BaseHoodieTableServiceClient` change: the expanded
write timeline included pending regular compaction and created an extra rollback instant. Both
lookup sites now exclude regular compaction while retaining log compaction. No existing Spark
test or assertion was edited. The correction passed all 4 reported rollback rows, 65 shared
write/recovery contracts, and 231 Java MOR/read/NBCC cases. The full Spark unit and functional
profiles then covered 965 cases: 953 passed, 12 skipped, zero failures/errors. See the evidence
document's final follow-up section for the logs and local root command. The full root reactor
remains unverified with these environment settings.

The subsequent missing Spark/Flink adapter dependencies were caused by omitting explicit engine
profile flags: automatic `java17` activation disabled the `activeByDefault` engine profiles and
removed their modules from the reactor. Always include `-Dscala-2.12 -Dspark3.5 -Dflink2.2` in this
configuration. Both adapters and their upstream modules were built/installed successfully, and the
51-module root reactor passed `validate`. No source or test changes were needed for that issue.

With the explicit Spark profile on Java 17, ORC 1.9.1's `orc-shims` POM adds Hadoop client API
3.3.5 without its matching runtime, causing missing shaded XML parser classes in client-common.
The runtime JAR was downloaded and supplied through `-Dmaven.test.additionalClasspath` only.
The full client-common suite passed: 1,423 tests, zero failures/errors, 2 skips. The evidence
document's final root command includes this extra classpath entry; no source, test, or POM changes
were made for this issue. The entire root test run with all flags remains unverified.

## Historical repository state at the end of the test-only phase

- Current implementation commit: `9c6cc679b8` (`enabled java standalone client to have missing capability for MoR`)
- Parent used when reviewing what the implementation actually introduced: `8e88496082`
- The current working tree contains test-only changes plus these handoff documents.
- Do not assume the current test suite is green. Several new tests intentionally fail because they
  demonstrate real gaps. The earlier statement that the fork CI matrix was green described the state
  before these hardening tests were introduced.
- `git diff --check` was clean at the end of the test work.

Modified test files:

- `hudi-client/hudi-java-client/src/test/java/org/apache/hudi/client/TestHoodieJavaReadClient.java`
- `hudi-client/hudi-java-client/src/test/java/org/apache/hudi/client/functional/TestHoodieJavaClientOnMergeOnReadStorage.java`
- `hudi-client/hudi-java-client/src/test/java/org/apache/hudi/client/functional/TestJavaNonBlockingConcurrencyControl.java`
- `hudi-common/src/test/java/org/apache/hudi/common/model/TestBaseAvroPayload.java`
- `hudi-common/src/test/java/org/apache/hudi/common/util/collection/TestLazyConcatenatingIterator.java`
- `hudi-client/hudi-java-client/src/test/java/org/apache/hudi/common/table/read/HoodieFileGroupReaderOnJavaTestBase.java`
- `hudi-client/hudi-java-client/src/test/java/org/apache/hudi/common/table/read/TestHoodieFileGroupReaderOnJava.java`
- `hudi-hadoop-common/src/test/java/org/apache/hudi/common/functional/TestHoodieLogFormat.java`
- `hudi-hadoop-common/src/test/java/org/apache/hudi/common/functional/TestHoodieLogFormatAppendFailure.java`
- `hudi-hadoop-common/src/test/java/org/apache/hudi/common/util/collection/TestBitCaskDiskMap.java`

See `JAVA_MOR_TEST_EVIDENCE.md` for the test-by-test evidence matrix and reproduction commands.

## Continuation on 2026-09-05: expanded parity tests

The user explicitly reaffirmed: **change tests only, not production source**. That was the active
scope for this historical continuation, which added 81 distinct test invocations
(including parameter rows and the expanded spill matrix) and corrected one pre-existing import-order
violation in the modified MOR functional test.

New coverage includes all eight basic Java write entry points on empty input, nonempty insert/upsert/
bulk/prepped lifecycles, independent prepped deletes, actual inflight marker/listing rollback, savepoint
restore across compaction and an abandoned writer, delete-all/reinsert, overlapping lazy snapshots,
event-time deletes, commit-time ordering, input deduplication, partition-scoped keys, overwrite replace
commits, metadata-vs-filesystem listing, small-log rollover, both spill backends with early close and
10,001 sequential child readers.

Additional defects are now covered by RED tests:

- Update-only and empty `upsertPreppedRecords` do not transition the requested instant to inflight.
- Partition/table overwrite after compaction loses replacement rows from snapshot/incremental results.
  The same operations on log-only input pass, including time travel, skip-overwrite filtering and rollback.
- `deletePartitions` is explicitly unsupported on the inherited Java table API.
- `LazyConcatenatingIterator` double-closes the prior reader if the next supplier throws.

The expanded evidence matrix and acceptance boundary are in `JAVA_MOR_TEST_EVIDENCE.md`. The later
remaining-gap continuation below supersedes the initial version/storage/fault/performance exclusions.
Do not claim literal all-edge-case or measured speed equivalence with Spark/Flink. Positive bootstrap,
TTL and log-compaction acceptance contracts still fail at unsupported operations; they must remain red
until implementation work is separately authorized.

For full Java-module validation, run Maven with permission to open localhost sockets: embedded timeline
server tests fail in the filesystem sandbox with `SocketException: Operation not permitted`. Normal
Maven test execution enables Checkstyle. Surefire retries **failed tests only**, up to three times; it
does not run every passing test four times. Final XML reports can contain only a failed retry subset,
so use Maven's aggregate summary and initial class summaries for accurate invocation counts.

## Most important findings

### 1. Regular Java MoR writing and compaction were not absent upstream

Do not claim that this branch creates all Java MoR write support from nothing. Normal Java MoR delta
writes and regular compaction were already added upstream by `21f112e2c8` on 2025-04-18
(`[HUDI-8072] Fix java client functionality for MOR tables`). The branch adds substantial read,
bucket-index/NBCC, and compaction/schema hardening, but the community-facing motivation must be more
precise.

The write-side gaps proven by the new tests are narrower and concrete:

- `insertPreppedRecords` takes an inherited CoW path and writes base files instead of the MoR
  delta-commit/log path.
- Java log-compaction planning works, including the same scheduling-order rules tested by Spark, but
  execution always throws `UnsupportedOperationException` from
  `HoodieJavaMergeOnReadTableCompactor#preCompact`.
- Both log-only file groups and logs appended over a compacted base file hit the same unsupported
  execution path.

Regular compaction, async writes around a pending compaction, inline scheduling, cleaner retention,
clustering preservation, and failed-writer compaction recovery have positive Java tests.

### 2. `HoodieJavaReadClient` works for the main path, but several boundaries are incomplete

Positive behavior now has focused coverage for:

- MoR snapshot merging before compaction.
- Read-optimized visibility before/after compaction.
- Time travel and completion-time incremental reads on the active timeline.
- Out-of-order completion and inflight-writer isolation under NBCC.
- A later delta commit while compaction output is inflight: read-optimized ignores the inflight base,
  while snapshot sees the later completed log update.
- Event-time ordering before and after compaction.
- Delete filtering.
- CoW sanity.
- Spill-to-disk and cleanup of the per-file-group merge buffer.
- CDC-enabled Java MoR writes being discoverable by Hudi's common CDC extractor.

The tests prove these remaining gaps:

- Time travel accepts a scheduled but incomplete compaction instant even though the API documents a
  completed target.
- An InternalSchema column rename over an old-schema log block returns `null` for the renamed field.
- The documented `populate.meta.fields=false` incremental limitation is not rejected at the API entry
  point.
- LSM storage is accepted even though the implementation always selects the normal file-group reader
  rather than the LSM-specific reader.

Archived incremental ranges are deliberately unsupported and already checked. An archived time-travel
assertion was also added to the archival test. Treat these as explicit v1 exclusions unless the planned
API scope changes.

### 3. The memory claim must be narrowed

The reader opens and closes one `HoodieFileGroupReader` at a time through
`LazyConcatenatingIterator`, and the file-group merge buffer can spill to disk. Tests verify both child
iterator laziness and spill-directory cleanup.

However, `HoodieJavaReadClient#collectMergedFileSlices` eagerly collects all selected file slices, and
`toRecordIterator` eagerly creates a supplier for each slice. Therefore, do not say memory is bounded
independently of table size. A defensible claim is:

> The record merge working set is bounded to one file group at a time and can spill to disk; planning
> memory still scales with the number of selected partitions/file groups.

If constant/streaming planning memory is a requirement, production code will need another change and a
separate scale-oriented test.

### 4. The CDC description needed correction

It is inaccurate to say that Java MoR writes cannot produce anything consumable by CDC. With CDC
enabled, the common `HoodieCDCExtractor` can infer both insert and update changes from ordinary MoR log
files using the `LOG_FILE` inference case.

What is actually absent is a CDC-output method on `HoodieJavaReadClient`. The read-client test class
Javadoc was corrected, but the production `HoodieJavaReadClient` Javadoc still contains the older,
inaccurate wording because production edits were outside this phase's authorization.

### 5. The shared `BaseAvroPayload` change is not complete

The original by-name projection fix works while the payload still has its in-memory writer schema, and
the reordered projection case passes. The new aggregate regression test exposes four additional cases:

1. After Kryo serialization, the transient writer schema is lost; projecting the restored bytes with a
   different schema produces malformed Avro data.
2. Reader-field aliases are rejected instead of participating in Avro schema resolution.
3. A nested rename is misclassified as a pure top-level projection and silently becomes `null`.
4. An added reader field with a default falls into positional decoding and produces an EOF-backed
   `HoodieIOException` when accessed.

This is shared code used by every engine. Keep it in a separate PR and resolve the serialization/schema
evolution contract before presenting it as a safe general fix. Any wire-format change to retain writer
schema across Kryo serialization requires explicit backward-compatibility analysis.

## Spark/Flink parity policy used

The audit did not copy every Spark or Flink test literally. It ported engine-neutral storage and client
contracts:

- base + log merging, log-only reads, deletes, ordering, and compaction visibility;
- active-timeline incremental range behavior and out-of-order completion;
- compaction/log-compaction scheduling, cleaner interaction, clustering preservation, and recovery;
- NBCC overlap, partial updates, inflight instants, bucket routing, and bulk-insert conflict cases;
- spill and resource-lifecycle behavior;
- CDC write interoperability at the common extractor boundary.

These are intentionally not Java-client parity requirements because there is no equivalent standalone
API:

- Spark SQL/DataFrame predicates, vectorized readers, Catalyst record types, and Spark datasource
  options;
- Flink operators, checkpoints, failover/recommit coordination, source split throttling, and RowData
  changelog modes;
- Flink CDC source output ordering (the Java read client has no CDC-output API);
- archived incremental reading, which is an explicit Java v1 limitation.

If a future agent ports another test, it should state the engine-neutral invariant being preserved. Do
not add tests that merely reproduce Spark/Flink framework plumbing in Java.

## Recommended next implementation order

No production fix should be made unless the user explicitly authorizes it. Once authorized:

1. Address the shared `BaseAvroPayload` cases in a standalone common-module PR. Re-run Hive/Presto MoR
   schema-evolution suites because positional compatibility was the reason for the hybrid decode logic.
2. Make the Java read-client boundaries honest: validate completed time-travel targets, fail fast for
   unsupported no-meta incremental and LSM modes, and add InternalSchema/FGR reconciliation if schema
   evolution remains in scope.
3. Override `insertPreppedRecords` routing for MoR.
4. Implement Java log compaction end to end, then validate the now-present rollback and archival
   acceptance tests. They currently stop at the unsupported execution call; their downstream fixture
   behavior will need validation once execution is implemented.
5. Either stream file-slice planning or narrow the public bounded-memory wording.
6. Correct the production CDC limitation Javadoc.

## PR/message wording guardrails

Safe wording:

- “Adds a standalone Java read client for MoR/CoW and closes Java-engine bucket-index/NBCC gaps.”
- “Uses one file-group reader at a time with spillable merge state.”
- “Regular Java MoR compaction existed upstream; this work adds read support and hardens/extends Java
  MoR integration.”
- “CDC output is not exposed by the v1 read API; CDC-enabled Java MoR writes remain inferable by the
  common CDC extractor.”

Avoid until the red tests are fixed:

- “full CI matrix green” for the current working tree;
- “complete MoR write support”;
- “log compaction works end to end”;
- “schema evolution supported”;
- “memory is bounded regardless of table size”;
- “Java writes do not produce CDC-consumable changes.”


## Final acceptance-suite expansion

The user clarified that completeness of the acceptance suite comes first, including tests that cannot
pass until implementation is added. The suite therefore now includes explicit log-compaction rollback/
archival, bootstrap, TTL, clustering/read boundaries, all cleaner policies, mixed-operation model
sequences, real multi-file-group spill limits, added schema defaults and no-meta snapshot contracts.

The model tests execute three fixed seeds with 48 transitions each and accumulate read assertions, so
an early failure does not prevent later steps from being exercised. They expose another implementation
gap: earliest-to-latest incremental reads can be empty/partial after compaction and rollback even when
snapshot rows exist. Java/Hive shared file-group reader suites and Java metadata suites are part of the
full Java-module verification; do not incorrectly describe them as untested.

Use the final verification section of `JAVA_MOR_TEST_EVIDENCE.md` for run counts. Test-only fixture
corrections made during development are not feature gaps. No production file was edited.


Final verified results:

- Full Java-module run: 340 cases, 325 pass, 15 fail, before the final acceptance additions.
- Final expanded read-client class: 104 cases, 80 pass, 24 fail.
- Union of the two Java runs (replace the old 81 read cases with the final 104): 363 distinct cases,
  336 pass and 27 fail. This is not a single-run aggregate.
- Shared cleaner/schema/payload/iterator run: 103 cases, 101 pass, 2 fail.
- Selected shared corruption suite: zero executed because external-HDFS setup was assumption-aborted.
  Do not describe its BUILD SUCCESS as a passing corruption test. It needs external HDFS on localhost:9000
  and `-Duse.external.hdfs=true`.
- Final Checkstyle and whitespace checks pass. Production files are unchanged.

The 24 read-class failures and exact commands are accounted for individually in the evidence file.
Use those tests as the next implementation backlog; keep test-only scope until the user authorizes
production fixes. New log-compaction/bootstrap/TTL acceptance bodies past the unsupported calls still
need runtime verification after implementation, rather than being assumed validated today.


## Remaining-gap continuation after “can we do all”

At the end of the historical test-only phase, the tests-only constraint remained in force. Read the final section of `JAVA_MOR_TEST_EVIDENCE.md` for
current results; earlier summaries above are historical. This phase adds supported-version/format/
codec lifecycles, all directed supported-version migrations, precise commit/metadata failure boundaries,
abrupt child-JVM death, a fault-injecting Hadoop filesystem, rollback interruption/retry, partial read
failure with real spill cleanup, measured/scalable workloads, strong OCC value checks and unsuppressed
shared added-field schema tests.

Important fixture details for the next agent:

- V10 native logs use the base format. Inline `hoodie.logfile.data.block.format` variants are counted
  only for v8/v9, with a separate v6 single-writer matrix.
- Version migrations must reopen `HoodieTableMetaClient` from storage. `reload(oldClient)` carries the
  old timeline-layout version and can manufacture a fixture error or empty reads after a migration.
- `JavaDeleteHelper` clears its input list. Matrix deletes supply mutable lists so this unrelated input
  contract does not block format lifecycle assertions.
- Inject at the Hadoop filesystem boundary to cover native Parquet, which can bypass custom
  `HoodieStorage` methods. Disable the plain `file` cache and invalidate the cached `hoodie-file` wrapper before each fault
  fixture: otherwise it retains an earlier plain filesystem and misses injections. Preserve wrapper
  caching within the test, because MDT HFile bootstrap tracks stream bytes through that same instance. Exclude partition-metadata creation when
  testing data-file failure.
- One-shot faults may legitimately be retried internally. Persistent create/write failure must still
  produce an error notification. The test completes rollback/retry checks before reporting that gap.
- MDT rollback rolls back its own delta commit. Injection targets MDT storage operations, including
  timeline files, rather than incorrectly requiring a fresh MDT data block on every rollback.
- `TestHoodieLogFormat` now supports `-Duse.embedded.hdfs=true` on this Java 17/Hadoop runtime. Twelve
  corruption/rollback invocations ran and passed. The separate four-DataNode failure test passed too.
  They require local sockets outside the restricted sandbox. No Docker/HDFS download was necessary.
- `TestBitCaskDiskMap` no longer disables size-estimation performance coverage. It warms up and checks
  10,000 estimates with a configurable coarse budget.
- Performance reports are in `hudi-client/hudi-java-client/target/mor-performance/`. Main-thread allocation
  is measured with HotSpot counters. Heap delta is observational, not peak heap. Time/allocation limits
  and workload scale are configurable; these are Java regressions, not cross-engine speed comparisons.

Do not disable newly red tests, weaken their expected outputs, or change production code during this
phase. The evidence matrix is the implementation backlog for the later authorized production work.


### Latest verified totals

- Final reader: **196 cases, 163 passing, 33 failing**, no flakes or skips after fixing the test wrapper cache.
- Shared Java file-group reader with added fields enabled: **23 passing**.
- Full module plus final class replacements: **455 distinct Java cases, 419 passing, 36 failing**.
  The full module itself ran 451 cases before the final four OCC additions and fixture refinements;
  do not describe the combined figure as a single final full-module run.
- Common acceptance contracts: **106 cases, 103 passing, three failing**.
- Selected Hadoop storage/HDFS coverage: **163 cases, all passing**, combining the final class results.
- Aggregate default coverage: **724 cases, 685 passing, 39 failing**. Cases are parameter invocations,
  not unique implementation defects. Keep failures visible for the later implementation phase.

New failures: six mixed-format Parquet-inline/ORC-or-HFile-base reader cases across v6/v8/v9, two
persistent CREATE/WRITE notification cases, MDT DELETE interruption recovery, and the additional
shared throwing-close iterator regression. All 12 migrations and all four strong OCC cases pass.

The performance workload was also executed at scale 4: all four cases pass, up to 8,192 keys with
4-KiB values and eight write batches, under explicit time/allocation budgets. See
`/tmp/java-mor-scaled-performance.log` and `target/mor-performance/` in the Java module.
