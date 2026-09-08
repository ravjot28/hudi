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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Deletes partitions through a replace commit, preserving time travel and rollback. */
public class JavaDeletePartitionCommitActionExecutor<T> extends JavaInsertOverwriteCommitActionExecutor<T> {
  private final List<String> partitions;

  public JavaDeletePartitionCommitActionExecutor(HoodieEngineContext context, HoodieWriteConfig config,
                                                 HoodieTable table, String instantTime, List<String> partitions) {
    super(context, config, table, instantTime, Collections.emptyList(), WriteOperationType.DELETE_PARTITION);
    this.partitions = partitions;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute() {
    DeletePartitionUtils.checkForPendingTableServiceActions(table, partitions);
    long start = System.nanoTime();
    Map<String, List<String>> replaced = new LinkedHashMap<>();
    partitions.forEach(partition -> replaced.computeIfAbsent(partition, this::getAllExistingFileIds));
    saveWorkloadProfileMetadataToInflight(
        new WorkloadProfile(Pair.of(Collections.emptyMap(), new WorkloadStat())), instantTime);
    HoodieWriteMetadata<List<WriteStatus>> result = new HoodieWriteMetadata<>();
    result.setPartitionToReplaceFileIds(replaced);
    result.setWriteStatuses(Collections.emptyList());
    result.setIndexUpdateDuration(Duration.ofNanos(System.nanoTime() - start));
    runPrecommitValidators(result);
    return result;
  }
}
