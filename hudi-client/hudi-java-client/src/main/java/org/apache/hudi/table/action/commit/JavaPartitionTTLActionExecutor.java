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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieDeletePartitionPendingTableServiceException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.ttl.strategy.HoodiePartitionTTLStrategyFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Uses the shared retention strategies and partition-service conflict checks. */
@Slf4j
public class JavaPartitionTTLActionExecutor<T> extends BaseJavaCommitActionExecutor<T> {
  public JavaPartitionTTLActionExecutor(HoodieEngineContext context, HoodieWriteConfig config,
                                        HoodieTable table, String instantTime) {
    super(context, config, table, instantTime, WriteOperationType.DELETE_PARTITION);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute() {
    try {
      List<String> expired = HoodiePartitionTTLStrategyFactory.createStrategy(table, config.getProps(), instantTime)
          .getExpiredPartitionPaths();
      if (!expired.isEmpty()) {
        JavaDeletePartitionCommitActionExecutor<T> executor =
            new JavaDeletePartitionCommitActionExecutor<>(context, config, table, instantTime, expired);
        HoodieWriteMetadata<List<WriteStatus>> result = executor.execute();
        executor.completeCommit(result);
        return result;
      }
    } catch (HoodieDeletePartitionPendingTableServiceException e) {
      log.info("Deferring partition expiration while a table service is pending", e);
    } catch (IOException e) {
      throw new HoodieIOException("Error executing partition TTL", e);
    }
    HoodieWriteMetadata<List<WriteStatus>> result = new HoodieWriteMetadata<>();
    result.setPartitionToReplaceFileIds(Collections.emptyMap());
    result.setWriteStatuses(Collections.emptyList());
    return result;
  }
}
