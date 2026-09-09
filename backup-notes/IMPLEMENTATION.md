# Native Java MOR review status

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
