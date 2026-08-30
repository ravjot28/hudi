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

import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.HoodieSimpleBucketIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Focused unit tests for the two hardening checks in {@link JavaBucketIndexPartitioner}: memoization of
 * {@link JavaBucketIndexPartitioner#getBucketInfo(int)} and the bounds check in
 * {@link JavaBucketIndexPartitioner#getPartition(Object)}.
 */
public class TestJavaBucketIndexPartitioner {

  private static final String PARTITION_PATH = "par1";

  private HoodieWriteConfig buildConfig(int numBuckets) {
    return HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withPath("/tmp/test-java-bucket-index-partitioner")
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .forTable("test")
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
            .withBucketNum(String.valueOf(numBuckets))
            .withRecordKeyField("_row_key")
            .withIndexKeyField("_row_key")
            .build())
        .build();
  }

  private HoodieTable mockTable(HoodieWriteConfig config) {
    HoodieTable table = mock(HoodieTable.class);
    when(table.getIndex()).thenReturn(new HoodieSimpleBucketIndex(config));
    return table;
  }

  /**
   * A single-partition, insert-only workload profile: no update locations, so every call to
   * {@link JavaBucketIndexPartitioner#getBucketInfo(int)} for a given bucket number would mint a brand-new
   * random fileId (via {@link BucketIdentifier#newBucketFileIdPrefix}) unless memoized.
   */
  private WorkloadProfile insertOnlyProfile() {
    WorkloadStat stat = new WorkloadStat();
    stat.addInserts(10);
    Map<String, WorkloadStat> partitionPathStatMap = new HashMap<>();
    partitionPathStatMap.put(PARTITION_PATH, stat);
    return new WorkloadProfile(Pair.of(partitionPathStatMap, new WorkloadStat()), WriteOperationType.INSERT, false);
  }

  @Test
  public void testGetBucketInfoIsMemoized() {
    HoodieWriteConfig config = buildConfig(4);
    HoodieTable table = mockTable(config);
    JavaBucketIndexPartitioner<?> partitioner =
        new JavaBucketIndexPartitioner<>(insertOnlyProfile(), null, table, config);

    BucketInfo first = partitioner.getBucketInfo(0);
    BucketInfo second = partitioner.getBucketInfo(0);

    // Same object reference: proves the second call was served from the memoization cache instead of
    // re-computing (and re-randomizing, via newBucketFileIdPrefix) a new BucketInfo.
    assertSame(first, second, "getBucketInfo must return the memoized BucketInfo on repeated calls "
        + "for the same bucket number");
    assertEquals(first.getFileIdPrefix(), second.getFileIdPrefix());
  }

  @Test
  public void testGetPartitionRejectsBucketIdBeyondConfiguredCount() {
    int numBuckets = 2;
    HoodieWriteConfig config = buildConfig(numBuckets);
    HoodieTable table = mockTable(config);
    JavaBucketIndexPartitioner<?> partitioner =
        new JavaBucketIndexPartitioner<>(insertOnlyProfile(), null, table, config);

    // Simulate an existing file group whose fileId was minted under a higher bucket count: bucket id 5 is
    // out of range for numBuckets=2.
    String fileIdFromHigherBucketCount = BucketIdentifier.newBucketFileIdPrefix(5);
    HoodieRecordLocation existingLocation = new HoodieRecordLocation("001", fileIdFromHigherBucketCount);
    Pair<HoodieKey, Option<HoodieRecordLocation>> keyLocation =
        Pair.of(new HoodieKey("any-key", PARTITION_PATH), Option.of(existingLocation));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> partitioner.getPartition(keyLocation));
    assertTrue(e.getMessage().contains("exceeds configured bucket count"), "unexpected message: " + e.getMessage());
    assertTrue(e.getMessage().contains("does not support changing hoodie.bucket.index.num.buckets"),
        "unexpected message: " + e.getMessage());
  }
}
