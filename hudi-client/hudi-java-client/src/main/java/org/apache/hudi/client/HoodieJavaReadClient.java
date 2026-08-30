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

import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.AvroReaderContextFactory;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.LazyConcatenatingIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A read client for the Java engine, completing the read side of the NBCC (Non-Blocking Concurrency
 * Control) work in {@code hudi-java-client}: snapshot, read-optimized, time-travel, and completion-time-based
 * incremental reads, for both MERGE_ON_READ and COPY_ON_WRITE tables.
 *
 * <p>Every method loads its own fresh {@link HoodieTableMetaClient} snapshot (via
 * {@link HoodieTableMetaClient#reload}) and builds a {@link HoodieTableFileSystemView} from it, so concurrent
 * or overlapping calls -- including a caller still iterating a previous call's result -- never observe each
 * other's timeline reloads; the {@code HoodieTableMetaClient} passed to the constructor is never itself
 * mutated. Each method returns a lazily-chained {@link ClosableIterator} over one {@link HoodieFileGroupReader}
 * per file slice (base file merged with its log files) -- readers are opened one at a time as the iterator is
 * consumed, so memory stays bounded regardless of table size.
 *
 * <p><b>The returned {@link ClosableIterator} is the only resource this class hands out; {@link #close()} on
 * this client itself is a no-op.</b> This client holds no state between calls, so there is nothing for it to
 * release -- but each iterator you get back from {@code readSnapshot}/{@code readOptimized}/
 * {@code readIncremental} owns live file handles and <b>must be closed by the caller</b>, including on the
 * partial-iteration/early-abort path (a {@code try-with-resources} on the iterator is the simplest way).
 *
 * <p>Snapshot isolation under NBCC comes from {@link HoodieTableFileSystemView} itself: for table version 8+,
 * {@code AbstractTableFileSystemView#getLatestMergedFileSlicesBeforeOrOn} strips log files belonging to
 * instants that have not completed yet as of the view's timeline snapshot ({@code filterUncommittedLogs}),
 * regardless of how those instants are ordered by requested time -- so a writer that is still inflight when a
 * snapshot/incremental read runs is invisible to that read, exactly matching the out-of-order-completion
 * semantics the rest of the NBCC write path relies on.
 *
 * <p><b>Known limitations:</b>
 * <ul>
 *   <li>{@link #readIncremental(String, String, IncrementalConfig)} throws {@link UnsupportedOperationException}
 *   when the requested completion-time range falls (even partially) into the archived timeline: correctly
 *   serving such a range requires either a metadata-table-backed full-snapshot fallback or reading archived
 *   commit metadata directly, neither of which is implemented here (v1). Narrow the range to the active
 *   timeline, or read the current snapshot via {@link #readSnapshot()} instead.</li>
 *   <li>Tables written with {@code hoodie.populate.meta.fields=false} are not supported by
 *   {@link #readIncremental(String, String)}: completion-time-based record filtering relies on the
 *   {@code _hoodie_commit_time} meta field being present in every record.</li>
 *   <li>Schema evolution ({@code InternalSchema}) is ignored: every read uses the latest table schema
 *   resolved via {@link TableSchemaResolver} for both the data and requested schema, with no per-file-slice
 *   schema reconciliation for columns added/renamed/dropped over the table's history.</li>
 *   <li>CDC (change-data-capture) reads are out of scope: they require CDC-enabled writes, which the Java
 *   engine's write path does not yet produce.</li>
 * </ul>
 */
public class HoodieJavaReadClient implements AutoCloseable {

  private final HoodieJavaEngineContext context;
  private final HoodieTableMetaClient metaClient;
  private final TypedProperties props;
  private final HoodieMetadataConfig metadataConfig;

  public HoodieJavaReadClient(HoodieJavaEngineContext context, HoodieTableMetaClient metaClient, TypedProperties props) {
    this.context = context;
    this.metaClient = metaClient;
    this.props = props;
    this.metadataConfig = HoodieMetadataConfig.newBuilder().fromProperties(props).build();
  }

  public HoodieJavaReadClient(HoodieJavaEngineContext context, String basePath, TypedProperties props) {
    this(context, HoodieTableMetaClient.builder().setBasePath(basePath).setConf(context.getStorageConf().newInstance()).build(), props);
  }

  /**
   * Reads the current snapshot of the table: for MERGE_ON_READ, the latest merged file slices (base file, if
   * any, merged with its log files); for COPY_ON_WRITE, the latest base files (there are no log files).
   */
  public ClosableIterator<HoodieRecord<IndexedRecord>> readSnapshot() {
    return readSnapshot(null);
  }

  /**
   * Time-travel read: same as {@link #readSnapshot()}, but as of the given completed instant's
   * <b>requested</b> time instead of the latest completed instant. Note this takes a requested time, unlike
   * {@link #readIncremental(String, String)}'s bounds, which are <b>completion</b> times -- the two time
   * domains are not interchangeable; passing a completion time here will (at best) throw and (at worst)
   * silently match the wrong instant if it happens to collide with some other instant's requested time.
   *
   * @param asOfInstant the requested time of a completed instant on the active timeline, or {@code null} to
   *                     read as of the latest completed instant (equivalent to {@link #readSnapshot()}).
   * @throws IllegalArgumentException if {@code asOfInstant} does not name a completed instant on the active
   *                                   timeline (archived time-travel targets are not supported).
   */
  public ClosableIterator<HoodieRecord<IndexedRecord>> readSnapshot(String asOfInstant) {
    HoodieTableMetaClient freshMetaClient = HoodieTableMetaClient.reload(metaClient);
    // filterCompletedAndCompactionInstants (not filterCompletedInstants): a scheduled-but-not-yet-run
    // compaction still needs to be on this timeline, otherwise HoodieFileGroup#isFileSliceCommitted rejects
    // any later-completing delta commit whose file slice's base instant is that pending compaction's
    // instant time, silently dropping its records from the snapshot (see the NOTE in
    // BaseHoodieTableFileIndex regarding real-time views over pending compaction).
    HoodieTimeline timeline = freshMetaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
    if (timeline.empty()) {
      return new EmptyIterator<>();
    }
    String maxInstantTime;
    if (asOfInstant != null) {
      ValidationUtils.checkArgument(timeline.getInstantsAsStream().anyMatch(i -> i.requestedTime().equals(asOfInstant)),
          () -> "No completed instant found with requested time " + asOfInstant + " on the active timeline to time-travel to");
      maxInstantTime = asOfInstant;
    } else {
      maxInstantTime = timeline.filterCompletedInstants().lastInstant()
          .map(HoodieInstant::requestedTime)
          .orElseGet(() -> timeline.lastInstant().get().requestedTime());
    }
    List<FileSlice> fileSlices = collectMergedFileSlices(freshMetaClient, timeline, listPartitions(freshMetaClient), maxInstantTime);
    return toRecordIterator(freshMetaClient, fileSlices, maxInstantTime, Option.empty());
  }

  /**
   * Reads the latest base files only, skipping log files entirely. On a MERGE_ON_READ table, a (partition,
   * file group) whose only file slice has no base file yet (e.g. an NBCC file group that has only ever been
   * appended to, before its first compaction) is invisible to this read -- exactly the "read-optimized" query
   * semantics: uncompacted updates are not reflected until the next compaction produces a base file. On
   * COPY_ON_WRITE this is equivalent to {@link #readSnapshot()}, since every file slice is base-file-only.
   */
  public ClosableIterator<HoodieRecord<IndexedRecord>> readOptimized() {
    HoodieTableMetaClient freshMetaClient = HoodieTableMetaClient.reload(metaClient);
    // See readSnapshot() for why filterCompletedAndCompactionInstants is required here too.
    HoodieTimeline timeline = freshMetaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
    if (timeline.empty()) {
      return new EmptyIterator<>();
    }
    String maxInstantTime = timeline.filterCompletedInstants().lastInstant()
        .map(HoodieInstant::requestedTime)
        .orElseGet(() -> timeline.lastInstant().get().requestedTime());
    List<FileSlice> baseFileSlices;
    try (HoodieTableFileSystemView view = buildFileSystemView(freshMetaClient, timeline)) {
      baseFileSlices = listPartitions(freshMetaClient).stream()
          .flatMap(partition -> view.getLatestBaseFilesBeforeOrOn(partition, maxInstantTime)
              .map(baseFile -> toBaseFileOnlySlice(partition, baseFile)))
          .collect(Collectors.toList());
    }
    return toRecordIterator(freshMetaClient, baseFileSlices, maxInstantTime, Option.empty());
  }

  /**
   * Completion-time-based incremental read: returns records written by instants whose <em>completion</em>
   * time falls within {@code [startCompletionTime, endCompletionTime]} (both bounds inclusive when given),
   * regardless of how those instants are ordered by requested (start) time -- so it is correct under NBCC's
   * out-of-order completion, unlike a requested-time-based range would be. Requires
   * {@code hoodie.populate.meta.fields=true} (see Known limitations on the class).
   *
   * <p>Boundary semantics (deliberately simple and self-consistent, chosen for this batch-style API). Note
   * these bounds are <b>completion</b> times, unlike {@link #readSnapshot(String)}'s requested-time argument:
   * <ul>
   *   <li>{@code startCompletionTime == null}: per {@link IncrementalQueryAnalyzer}'s own default ("usual
   *   streaming read semantics"), this does <b>not</b> mean "from the beginning" -- it returns only the
   *   single latest completed instant. Pass {@link IncrementalQueryAnalyzer#START_COMMIT_EARLIEST}
   *   ({@code "earliest"}) explicitly to read from the beginning of the table's history.</li>
   *   <li>{@code startCompletionTime == "earliest"} and {@code endCompletionTime == null}: this is a
   *   <b>latest-snapshot</b> read (equivalent to {@link #readSnapshot()}), not a record-by-record walk of
   *   the table's full history -- there is no per-record instant filtering in this case, matching
   *   {@link IncrementalQueryAnalyzer}'s own "[earliest, +INF] -> the latest snapshot files" semantics.</li>
   *   <li>{@code endCompletionTime == null} (with a non-earliest start): open-ended, i.e. up to and
   *   including the latest completed instant.</li>
   *   <li>Both bounds, when non-null, are inclusive (a single completion time may be passed as both start
   *   and end to select exactly one instant's records).</li>
   * </ul>
   *
   * @param startCompletionTime inclusive lower bound on completion time, {@code "earliest"} for full
   *                             history, or {@code null} (see above).
   * @param endCompletionTime   inclusive upper bound on completion time, or {@code null} for open-ended.
   */
  public ClosableIterator<HoodieRecord<IndexedRecord>> readIncremental(String startCompletionTime, String endCompletionTime) {
    return readIncremental(startCompletionTime, endCompletionTime, IncrementalConfig.DEFAULT);
  }

  /**
   * Same as {@link #readIncremental(String, String)}, with control over which commit-timeline actions are
   * skipped (defaults mirror Flink's streaming-read defaults: compaction and clustering instants are skipped
   * -- so a compaction landing inside the range does not surface duplicate records for the data it merged --
   * insert-overwrite instants are not skipped).
   */
  public ClosableIterator<HoodieRecord<IndexedRecord>> readIncremental(String startCompletionTime, String endCompletionTime,
                                                                       IncrementalConfig config) {
    HoodieTableMetaClient freshMetaClient = HoodieTableMetaClient.reload(metaClient);
    IncrementalQueryAnalyzer analyzer = IncrementalQueryAnalyzer.builder()
        .metaClient(freshMetaClient)
        .startCompletionTime(startCompletionTime)
        .endCompletionTime(endCompletionTime)
        .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .skipCompaction(config.isSkipCompaction())
        .skipClustering(config.isSkipClustering())
        .skipInsertOverwrite(config.isSkipInsertOverwrite())
        .build();
    IncrementalQueryAnalyzer.QueryContext queryContext = analyzer.analyze();
    if (queryContext.isEmpty()) {
      return new EmptyIterator<>();
    }
    if (!queryContext.getArchivedInstants().isEmpty()) {
      throw new UnsupportedOperationException("Incremental read from " + startCompletionTime + " to " + endCompletionTime
          + " falls (at least partially) into the archived timeline, which HoodieJavaReadClient does not support: "
          + "correctly serving it requires either a metadata-table-backed full-snapshot fallback or reading archived "
          + "commit metadata directly, neither of which is implemented on the Java engine yet. Narrow the range to "
          + "the active timeline, or use readSnapshot() to read the current state instead.");
    }
    String maxCompletionTime = queryContext.getMaxCompletionTime();
    Option<InstantRange> instantRangeOpt = resolveInstantRange(queryContext);
    // Use the full commits-and-compaction timeline (not the possibly compaction-filtered activeTimeline
    // carried by QueryContext when skipCompaction=true) to build the file system view: file slice
    // boundaries are derived from compaction instants, and slicing against a filtered timeline would
    // mis-classify them and lose data -- mirrors Flink's IncrementalInputSplits#getFullCommitsTimeline.
    // The skipCompaction/skipClustering/skipInsertOverwrite semantics are still honored, since they only
    // affect which instants make it into instantRangeOpt's record-level filter, not file slicing.
    HoodieTimeline fileSliceTimeline = freshMetaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
    List<String> partitions = partitionsForIncrementalRead(queryContext);
    List<FileSlice> fileSlices = collectMergedFileSlices(freshMetaClient, fileSliceTimeline, partitions, maxCompletionTime);
    return toRecordIterator(freshMetaClient, fileSlices, maxCompletionTime, instantRangeOpt);
  }

  /**
   * {@link IncrementalQueryAnalyzer.QueryContext#getInstantRange()} builds a REQUESTED-time-only upper bound
   * (no lower bound at all) for the "[earliest, endCompletionTime]" case, since that's the one case where the
   * analyzer expects callers to fall back to a full table scan rather than trust per-record filtering (see
   * its class javadoc table). We always rely on per-record filtering, so that requested-time bound is unsafe
   * under NBCC: a writer with an early requested time but a late completion time (after endCompletionTime)
   * would pass a requested-time check even though its own completion falls outside the queried range. Route
   * this one case through an EXACT_MATCH range over the exact (completion-time-correct) instant set the
   * analyzer already resolved instead of trusting getInstantRange()'s convenience result.
   */
  private static Option<InstantRange> resolveInstantRange(IncrementalQueryAnalyzer.QueryContext queryContext) {
    if (queryContext.isConsumingFromEarliest() && !queryContext.isConsumingToLatest()) {
      return Option.of(InstantRange.builder()
          .rangeType(InstantRange.RangeType.EXACT_MATCH)
          .explicitInstants(new HashSet<>(queryContext.getInstantTimeList()))
          .build());
    }
    return queryContext.getInstantRange();
  }

  @Override
  public void close() {
    // This client holds no resources between calls (see the class javadoc): each read method loads its own
    // fresh HoodieTableMetaClient and builds/closes its own HoodieTableFileSystemView before returning. The
    // returned ClosableIterator is the actual resource and is the caller's responsibility to close.
  }

  // -------------------------------------------------------------------------
  //  Helpers
  // -------------------------------------------------------------------------

  /**
   * Restricts the incremental read to the partitions actually touched by the in-range commits, instead of
   * scanning every partition in the table, mirroring Flink's {@code IncrementalInputSplits#getReadPartitions}
   * (derived from {@code WriteProfiles.getCommitMetadata}). Package-private so tests can assert on it
   * directly rather than only inferring it from returned data.
   */
  List<String> partitionsForIncrementalRead(IncrementalQueryAnalyzer.QueryContext queryContext) {
    HoodieTimeline activeTimeline = queryContext.getActiveTimeline();
    List<HoodieCommitMetadata> metadataList = new ArrayList<>(queryContext.getActiveInstants().size());
    for (HoodieInstant instant : queryContext.getActiveInstants()) {
      try {
        metadataList.add(activeTimeline.readCommitMetadata(instant));
      } catch (IOException e) {
        throw new HoodieIOException("Failed to read commit metadata for instant " + instant, e);
      }
    }
    Set<String> partitions = HoodieTableMetadataUtil.getWritePartitionPaths(metadataList);
    return new ArrayList<>(partitions);
  }

  private List<FileSlice> collectMergedFileSlices(HoodieTableMetaClient freshMetaClient, HoodieTimeline timeline,
                                                   List<String> partitions, String maxInstantTime) {
    try (HoodieTableFileSystemView view = buildFileSystemView(freshMetaClient, timeline)) {
      return partitions.stream()
          .flatMap(partition -> view.getLatestMergedFileSlicesBeforeOrOn(partition, maxInstantTime))
          .collect(Collectors.toList());
    }
  }

  private HoodieTableFileSystemView buildFileSystemView(HoodieTableMetaClient freshMetaClient, HoodieTimeline timeline) {
    return FileSystemViewManager.createInMemoryFileSystemViewWithTimeline(context, freshMetaClient, metadataConfig, timeline);
  }

  private List<String> listPartitions(HoodieTableMetaClient freshMetaClient) {
    return FSUtils.getAllPartitionPaths(context, freshMetaClient, metadataConfig);
  }

  private static FileSlice toBaseFileOnlySlice(String partitionPath, HoodieBaseFile baseFile) {
    FileSlice slice = new FileSlice(partitionPath, baseFile.getCommitTime(), baseFile.getFileId());
    slice.setBaseFile(baseFile);
    return slice;
  }

  private ClosableIterator<HoodieRecord<IndexedRecord>> toRecordIterator(HoodieTableMetaClient freshMetaClient, List<FileSlice> fileSlices,
                                                                          String latestCommitTime, Option<InstantRange> instantRangeOpt) {
    if (fileSlices.isEmpty()) {
      return new EmptyIterator<>();
    }
    HoodieSchema schema = getTableSchema(freshMetaClient);
    List<Supplier<ClosableIterator<HoodieRecord<IndexedRecord>>>> suppliers = new ArrayList<>(fileSlices.size());
    for (FileSlice fileSlice : fileSlices) {
      suppliers.add(() -> openFileSlice(freshMetaClient, fileSlice, schema, latestCommitTime, instantRangeOpt));
    }
    return new LazyConcatenatingIterator<>(suppliers);
  }

  private ClosableIterator<HoodieRecord<IndexedRecord>> openFileSlice(HoodieTableMetaClient freshMetaClient, FileSlice fileSlice,
                                                                       HoodieSchema schema, String latestCommitTime,
                                                                       Option<InstantRange> instantRangeOpt) {
    // A fresh reader context per file slice: HoodieReaderContext is stateful (setLatestCommitTime,
    // setHasLogFiles, schema handler, etc. are all mutated as a side effect of building the reader), so it
    // cannot be shared/reused across file group readers.
    HoodieReaderContext<IndexedRecord> readerContext = new AvroReaderContextFactory(
        freshMetaClient, freshMetaClient.getTableConfig().getPayloadClass(), instantRangeOpt, props).getContext();
    HoodieFileGroupReader<IndexedRecord> reader = HoodieFileGroupReader.<IndexedRecord>builder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(freshMetaClient)
        .withBaseFileOption(fileSlice.getBaseFile())
        .withLogFiles(fileSlice.getLogFiles())
        .withPartitionPath(fileSlice.getPartitionPath())
        .withDataSchema(schema)
        .withRequestedSchema(schema)
        .withLatestCommitTime(latestCommitTime)
        .withProps(props)
        .build();
    // If opening the record iterator fails for any reason, the reader (and whatever file handles it already
    // opened while building the schema handler) must still be closed before propagating -- otherwise this is
    // a resource leak on every failed file slice, not just the first one hit.
    try {
      return reader.getClosableHoodieRecordIterator();
    } catch (Throwable t) {
      try {
        reader.close();
      } catch (IOException closeException) {
        t.addSuppressed(closeException);
      }
      if (t instanceof IOException) {
        throw new HoodieIOException("Failed to open record iterator for file slice " + fileSlice, (IOException) t);
      }
      if (t instanceof RuntimeException) {
        throw (RuntimeException) t;
      }
      if (t instanceof Error) {
        throw (Error) t;
      }
      throw new RuntimeException("Failed to open record iterator for file slice " + fileSlice, t);
    }
  }

  private HoodieSchema getTableSchema(HoodieTableMetaClient freshMetaClient) {
    try {
      return new TableSchemaResolver(freshMetaClient).getTableSchema();
    } catch (Exception e) {
      throw new HoodieException("Unable to resolve table schema for table " + freshMetaClient.getBasePath(), e);
    }
  }

  /**
   * Controls which commit-timeline actions {@link #readIncremental(String, String, IncrementalConfig)} skips
   * when resolving the instants in range. Defaults mirror Flink's streaming-read defaults
   * ({@code read.streaming.skip_compaction}/{@code skip_clustering} default {@code true},
   * {@code skip_insertoverwrite} defaults {@code false}).
   */
  public static final class IncrementalConfig {

    public static final IncrementalConfig DEFAULT = new IncrementalConfig(true, true, false);

    private final boolean skipCompaction;
    private final boolean skipClustering;
    private final boolean skipInsertOverwrite;

    private IncrementalConfig(boolean skipCompaction, boolean skipClustering, boolean skipInsertOverwrite) {
      this.skipCompaction = skipCompaction;
      this.skipClustering = skipClustering;
      this.skipInsertOverwrite = skipInsertOverwrite;
    }

    public boolean isSkipCompaction() {
      return skipCompaction;
    }

    public boolean isSkipClustering() {
      return skipClustering;
    }

    public boolean isSkipInsertOverwrite() {
      return skipInsertOverwrite;
    }

    public static Builder builder() {
      return new Builder();
    }

    public static final class Builder {
      private boolean skipCompaction = true;
      private boolean skipClustering = true;
      private boolean skipInsertOverwrite = false;

      public Builder skipCompaction(boolean skipCompaction) {
        this.skipCompaction = skipCompaction;
        return this;
      }

      public Builder skipClustering(boolean skipClustering) {
        this.skipClustering = skipClustering;
        return this;
      }

      public Builder skipInsertOverwrite(boolean skipInsertOverwrite) {
        this.skipInsertOverwrite = skipInsertOverwrite;
        return this;
      }

      public IncrementalConfig build() {
        return new IncrementalConfig(skipCompaction, skipClustering, skipInsertOverwrite);
      }
    }
  }
}
