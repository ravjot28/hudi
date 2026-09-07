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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.fs.FileNameParser;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.SortUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.HoodieSimpleBucketIndex;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.table.BucketIndexBulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bulk-insert partitioner for the SIMPLE bucket index engine on the Java engine, mirroring Spark's
 * {@code RDDSimpleBucketBulkInsertPartitioner}. Records are grouped by the (partition path, bucket) they
 * hash into: a bucket that already has a file group appends to it (log file), while a brand-new bucket gets
 * a new file group -- with a deterministic fileId (based only on the bucket id) under non-blocking
 * concurrency control, so that two concurrent writers routing records into the same new bucket are
 * recognized as touching the same fileId by {@code BucketIndexConcurrentFileWritesConflictResolutionStrategy},
 * or a random fileId otherwise. Within each group, records are sorted the same way Spark's
 * {@code RDDBucketIndexPartitioner#doPartition} does: by the configured sort columns when set, else by
 * record key whenever {@link #arePartitionRecordsSorted()} requires it.
 */
public class JavaBucketIndexBulkInsertPartitioner<T> extends BucketIndexBulkInsertPartitioner<List<HoodieRecord<T>>> {

  public static final Logger LOG = LogManager.getLogger(JavaBucketIndexBulkInsertPartitioner.class);

  private final boolean isNonBlockingConcurrencyControl;
  private final NumBucketsFunction numBucketsFunction;

  /**
   * Records grouped by the (partition path, bucket) group they were routed to in the last call to
   * {@link #repartitionRecords}; the index into this list matches the index into the base class'
   * {@code fileIdPfxList}/{@code doAppend}, i.e. the {@code partitionId} passed to
   * {@link #getFileIdPfx(int)}/{@link #getWriteHandleFactory(int)}.
   */
  private List<List<HoodieRecord<T>>> recordsByGroup;

  public JavaBucketIndexBulkInsertPartitioner(HoodieTable table) {
    super(table, null, false);
    ValidationUtils.checkArgument(table.getIndex() instanceof HoodieSimpleBucketIndex,
        "JavaBucketIndexBulkInsertPartitioner should only be used with the SIMPLE bucket index engine");
    HoodieWriteConfig writeConfig = table.getConfig();
    ValidationUtils.checkArgument(StringUtils.isNullOrEmpty(writeConfig.getBucketIndexPartitionExpression()),
        "Partition-level bucket index (hoodie.bucket.index.partition.expressions) is not supported for the Java engine");
    this.isNonBlockingConcurrencyControl = writeConfig.isNonBlockingConcurrencyControl();
    this.numBucketsFunction = NumBucketsFunction.fromWriteConfig(writeConfig);
  }

  @Override
  public List<HoodieRecord<T>> repartitionRecords(List<HoodieRecord<T>> records, int outputPartitions) {
    // Rebuild all per-group state from scratch so that calling this method more than once on the same
    // instance is idempotent: fileIdPfxList/doAppend are inherited from the base class and would otherwise
    // keep growing across calls, drifting out of sync with the group indices computed below (which always
    // start fresh at 0).
    fileIdPfxList.clear();
    doAppend.clear();

    HoodieSimpleBucketIndex index = (HoodieSimpleBucketIndex) table.getIndex();
    // partition path -> (bucket id -> existing file group location), loaded lazily and only once per
    // partition path actually present in this batch
    Map<String, Map<Integer, HoodieRecordLocation>> existingBucketsByPartition = new HashMap<>();
    // partition path -> (bucket id -> group index into recordsByGroup/fileIdPfxList/doAppend)
    Map<String, Map<Integer, Integer>> groupIndexByPartitionAndBucket = new HashMap<>();
    List<List<HoodieRecord<T>>> groups = new ArrayList<>();

    for (HoodieRecord<T> record : records) {
      String partitionPath = record.getPartitionPath();
      int numBuckets = numBucketsFunction.getNumBuckets(partitionPath);
      int bucketId = index.getBucketID(record.getKey(), numBuckets);

      Map<Integer, Integer> groupIndexByBucket =
          groupIndexByPartitionAndBucket.computeIfAbsent(partitionPath, p -> new HashMap<>());
      Integer groupIndex = groupIndexByBucket.get(bucketId);
      if (groupIndex == null) {
        Map<Integer, HoodieRecordLocation> existingBuckets = existingBucketsByPartition.computeIfAbsent(
            partitionPath, p -> index.loadBucketIdToFileIdMappingForPartition(table, p));
        HoodieRecordLocation existingLocation = existingBuckets.get(bucketId);
        String fileIdPrefix;
        boolean append;
        if (existingLocation != null) {
          // Bucket file groups always have exactly one base file, written with write token 0 (see
          // BucketIndexBulkInsertPartitioner#getWriteHandleFactory / FSUtils#createNewFileId(prefix, 0)), so
          // stripping the write token here to get just the prefix and letting a fresh AppendHandleFactory
          // mint the append's own file name is safe and matches Spark's RDDSimpleBucketBulkInsertPartitioner.
          fileIdPrefix = FileNameParser.getFileIdPfxFromFileId(existingLocation.getFileId());
          append = true;
        } else {
          fileIdPrefix = BucketIdentifier.newBucketFileIdPrefix(bucketId, isNonBlockingConcurrencyControl);
          append = false;
        }
        groupIndex = fileIdPfxList.size();
        fileIdPfxList.add(fileIdPrefix);
        doAppend.add(append);
        groupIndexByBucket.put(bucketId, groupIndex);
        groups.add(new ArrayList<>());
      }
      groups.get(groupIndex).add(record);
    }

    sortGroupsIfNeeded(groups);
    this.recordsByGroup = groups;
    // The only consumer of this partitioner (JavaBulkInsertHelper) reads the grouped records back via
    // getRecordsByGroup() instead of this return value, but the BulkInsertPartitioner contract still
    // requires the returned list to actually be repartitioned (and sorted, when arePartitionRecordsSorted()
    // says so) for any future caller that relies on it directly. Flatten the already-built `groups` (an
    // ArrayList of references, not a deep copy) instead of returning the original, ungrouped/unsorted
    // `records` list.
    List<HoodieRecord<T>> flattened = new ArrayList<>(records.size());
    groups.forEach(flattened::addAll);
    return flattened;
  }

  /**
   * Sorts each (partition path, bucket) group in place, mirroring Spark's
   * {@code RDDBucketIndexPartitioner#doPartition}: by the configured sort columns when set
   * ({@link #isCustomSorted()}), else by record key whenever {@link #arePartitionRecordsSorted()} requires
   * it via {@link #isRecordKeySorted()}. This is required to honor the {@link #arePartitionRecordsSorted()}
   * contract inherited from {@code BucketSortBulkInsertPartitioner}.
   */
  private void sortGroupsIfNeeded(List<List<HoodieRecord<T>>> groups) {
    if (isCustomSorted()) {
      HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(HoodieSchema.parse(table.getConfig().getSchema()));
      boolean suffixRecordKey = table.getConfig().getBoolean(HoodieWriteConfig.BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS);
      groups.forEach(group -> group.sort((r1, r2) ->
          SortUtils.getComparableSortColumns(r1, sortColumnNames, schema, suffixRecordKey, consistentLogicalTimestampEnabled)
              .compareTo(SortUtils.getComparableSortColumns(r2, sortColumnNames, schema, suffixRecordKey, consistentLogicalTimestampEnabled))));
    } else if (isRecordKeySorted()) {
      if (table.getConfig().getBulkInsertSortMode() == BulkInsertSortMode.GLOBAL_SORT) {
        LOG.warn("Bucket index does not support global sort mode, the sort will only be done within each bucket file group");
      }
      groups.forEach(group -> group.sort((r1, r2) -> r1.getRecordKey().compareTo(r2.getRecordKey())));
    }
  }

  /**
   * @return the records grouped by the (partition path, bucket) group they were routed to, indexed the same
   *     way as {@link #getFileIdPfx(int)}/{@link #getWriteHandleFactory(int)}.
   */
  public List<List<HoodieRecord<T>>> getRecordsByGroup() {
    return recordsByGroup;
  }
}
