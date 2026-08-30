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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.HoodieBucketIndex;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.common.model.WriteOperationType.INSERT_OVERWRITE;
import static org.apache.hudi.common.model.WriteOperationType.INSERT_OVERWRITE_TABLE;

/**
 * Packs incoming records to be inserted/updated into buckets (1 bucket = 1 partition), for tables using
 * bucket index. Existing bucket file groups get an UPDATE bucket keyed by the existing fileId, while new
 * buckets get an INSERT bucket, except when non-blocking concurrency control is enabled, in which case new
 * buckets get an UPDATE bucket with a deterministic, NBCC-specific fileId so that concurrent writers agree
 * on the fileId and always write into the log file instead of a base file.
 */
public class JavaBucketIndexPartitioner<T> implements Partitioner, JavaBucketInfoGetter {

  private final int numBuckets;
  private final List<String> indexKeyFieldList;
  private final List<String> partitionPaths;
  /**
   * Helps get the partition id, partition id is partition offset + bucket id.
   * The partition offset is a multiple of the bucket num.
   */
  private final Map<String, Integer> partitionPathOffset;
  private final boolean isOverwrite;
  private final boolean isNonBlockingConcurrencyControl;

  /**
   * Partition path and file groups in it pair. Decide the file group an incoming update should go to.
   */
  private final Map<String, Set<String>> updatePartitionPathFileIds;

  /**
   * Memoizes the {@link BucketInfo} computed for a given partition number, so that repeated calls for the
   * same partition number (e.g. across multiple record batches routed to the same bucket) return the same
   * fileId instead of minting a new random one every time (see {@link BucketIdentifier#newBucketFileIdPrefix}).
   */
  private final Map<Integer, BucketInfo> bucketInfoCache = new ConcurrentHashMap<>();

  public JavaBucketIndexPartitioner(WorkloadProfile profile,
                                    HoodieEngineContext context,
                                    HoodieTable table,
                                    HoodieWriteConfig config) {
    if (!(table.getIndex() instanceof HoodieBucketIndex)) {
      throw new HoodieException(
          " Bucket index partitioner should only be used by BucketIndex other than "
              + table.getIndex().getClass().getSimpleName());
    }
    ValidationUtils.checkArgument(StringUtils.isNullOrEmpty(config.getBucketIndexPartitionExpression()),
        "Partition-level bucket index (hoodie.bucket.index.partition.expressions) is not supported for the Java engine");
    this.numBuckets = ((HoodieBucketIndex) table.getIndex()).getNumBuckets();
    this.indexKeyFieldList = KeyGenUtils.getIndexKeyFields(config.getBucketIndexHashField());
    this.partitionPaths = new ArrayList<>(profile.getPartitionPaths());
    this.partitionPathOffset = new HashMap<>();
    int offset = 0;
    for (String partitionPath : partitionPaths) {
      partitionPathOffset.put(partitionPath, offset);
      offset += numBuckets;
    }
    WriteOperationType operationType = profile.getOperationType();
    this.isOverwrite = INSERT_OVERWRITE.equals(operationType) || INSERT_OVERWRITE_TABLE.equals(operationType);
    this.isNonBlockingConcurrencyControl = config.isNonBlockingConcurrencyControl();
    if (isOverwrite) {
      ValidationUtils.checkArgument(!isNonBlockingConcurrencyControl,
          "Insert overwrite is not supported with non-blocking concurrency control");
    }
    this.updatePartitionPathFileIds = assignUpdates(profile);
  }

  private Map<String, Set<String>> assignUpdates(WorkloadProfile profile) {
    Map<String, Set<String>> partitionPathFileIds = new HashMap<>();
    // each update location gets a partition
    Set<Entry<String, WorkloadStat>> partitionStatEntries = profile.getInputPartitionPathStatMap().entrySet();
    for (Entry<String, WorkloadStat> partitionStat : partitionStatEntries) {
      partitionPathFileIds.computeIfAbsent(partitionStat.getKey(), k -> new HashSet<>());
      for (Entry<String, Pair<String, Long>> updateLocEntry :
          partitionStat.getValue().getUpdateLocationToCount().entrySet()) {
        partitionPathFileIds.get(partitionStat.getKey()).add(updateLocEntry.getKey());
      }
    }
    return partitionPathFileIds;
  }

  @Override
  public int getNumPartitions() {
    return partitionPaths.size() * numBuckets;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int getPartition(Object key) {
    Pair<HoodieKey, Option<HoodieRecordLocation>> keyLocation = (Pair<HoodieKey, Option<HoodieRecordLocation>>) key;
    String partitionPath = keyLocation.getLeft().getPartitionPath();
    Option<HoodieRecordLocation> location = keyLocation.getRight();
    int bucketId = location.isPresent()
        ? BucketIdentifier.bucketIdFromFileId(location.get().getFileId())
        : BucketIdentifier.getBucketId(keyLocation.getLeft().getRecordKey(), indexKeyFieldList, numBuckets);
    // Guard against a bucket id that no longer fits the configured bucket count (e.g. an existing file's
    // fileId was minted under a higher hoodie.bucket.index.num.buckets): silently letting it through would
    // make the record spill into another partition's slot instead of failing loudly. This runs on the
    // per-record hot path, so the message is built lazily (only on failure) via the Supplier overload.
    ValidationUtils.checkArgument(bucketId < numBuckets,
        () -> "Bucket id " + bucketId + " from existing file group exceeds configured bucket count " + numBuckets
            + " for partition " + partitionPath + ". Simple bucket index does not support changing "
            + "hoodie.bucket.index.num.buckets on an existing table; restore the original bucket count or "
            + "rebuild the table (consistent hashing resize is not supported on the Java engine).");
    return partitionPathOffset.get(partitionPath) + bucketId;
  }

  @Override
  public BucketInfo getBucketInfo(int bucketNumber) {
    return bucketInfoCache.computeIfAbsent(bucketNumber, this::computeBucketInfo);
  }

  private BucketInfo computeBucketInfo(int bucketNumber) {
    String partitionPath = partitionPaths.get(bucketNumber / numBuckets);
    String bucketIdStr = BucketIdentifier.bucketIdStr(bucketNumber % numBuckets);
    // Insert overwrite always generates new bucket file id
    if (isOverwrite) {
      return new BucketInfo(BucketType.INSERT, BucketIdentifier.newBucketFileIdPrefix(bucketIdStr), partitionPath);
    }
    Option<String> fileIdOption = Option.fromJavaOptional(updatePartitionPathFileIds
        .getOrDefault(partitionPath, Collections.emptySet()).stream()
        .filter(e -> e.startsWith(bucketIdStr))
        .findFirst());
    if (fileIdOption.isPresent()) {
      return new BucketInfo(BucketType.UPDATE, fileIdOption.get(), partitionPath);
    } else {
      // Always write into log file instead of base file if using NB-CC
      if (isNonBlockingConcurrencyControl) {
        return new BucketInfo(BucketType.UPDATE, BucketIdentifier.newBucketFileIdForNBCC(bucketIdStr), partitionPath);
      }
      return new BucketInfo(BucketType.INSERT, BucketIdentifier.newBucketFileIdPrefix(bucketIdStr), partitionPath);
    }
  }
}
