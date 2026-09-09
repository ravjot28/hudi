### Describe the issue this Pull Request addresses

Standalone Java MOR needs permanent coverage of native RecordMerger modes. A custom merger throwing during initial log scanning can leave an allocated spill buffer without an owner. Delete-key reduction also mutates the list supplied by the public Java write client, failing for immutable lists.

Related discussion: https://github.com/apache/hudi/discussions/19798
This is the incremental review description for the native-merger changes to fold into the prepared Java MOR read/write contribution; its local parent contains that contribution.

### Summary and Changelog

- Add 47 native Avro cases for event-time, commit-time and custom merging, ordering/deletes, rollback, regular/log compaction, partial updates, schema evolution, time travel/incremental reading and actual BitCask/RocksDB spill cleanup, including injected merger failures.
- Close a newly allocated record buffer when initial log scanning fails, preserving the original exception.
- Copy delete keys at the public Java write-client boundary; retain the internal helper's existing mutation contract and unchanged test.
- Add a standalone native Avro MOR example that demonstrates inserts, updates, deletion, snapshot reads and compaction.

### Impact

The native path is validated with the original BaseAvroPayload implementation as well as the proposed legacy fix. No payload wire-format change is introduced by this incremental commit. Closing a failed buffer affects the shared file-group reader. Copying delete input adds one list allocation proportional to delete batch size. No throughput improvement or production SLA is claimed.

### Risk Level

Medium: buffer cleanup is shared across engines. Common loader/merger tests and Java integration tests cover the changed lifecycle, including real spill implementations. Final validation results are recorded in IMPLEMENTATION.md. This incremental revision has not run a new remote CI matrix.

A separate regression reproduces a shared event-time IGNORE_DEFAULTS partial-update result changing when the first row is in a base file. Its expected values are preserved on pr/java-mor-partial-update-regression; that branch is known failing and is not part of this passing native suite. This contribution does not claim exhaustive MOR parity.

### Documentation Update

The prepared website guide explains native records, merger configuration and partial-update limitations, and links the new example. Website build and 26 generated links/anchors pass.

### Contributor's checklist

- [x] Read through contributor's guide during contribution preparation
- [x] Enough context is provided in the sections above
- [x] Adequate tests were added
