/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Reproduces the shared merge grouping difference without a Java writer or file-group reader. */
class TestPartialUpdateMergeGrouping {
  @Test
  void testCompactionMustNotChangeOutOfOrderPartialUpdateResults() throws Exception {
    HoodieSchema schema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"row\",\"fields\":["
        + "{\"name\":\"ts\",\"type\":\"long\"},"
        + "{\"name\":\"value\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
    AvroRecordContext recordContext = new AvroRecordContext();
    HoodieReaderContext<IndexedRecord> readerContext = mock(HoodieReaderContext.class);
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    BufferedRecordMerger<IndexedRecord> merger = BufferedRecordMergerFactory.create(readerContext,
        RecordMergeMode.EVENT_TIME_ORDERING, false, Option.empty(), Option.empty(), schema,
        new TypedProperties(), Option.of(PartialUpdateMode.IGNORE_DEFAULTS));
    BufferedRecord<IndexedRecord> base = record(schema, recordContext, 50, "preserve");
    BufferedRecord<IndexedRecord> patch = record(schema, recordContext, 60, null);
    BufferedRecord<IndexedRecord> late = record(schema, recordContext, 10, "late");

    BufferedRecord<IndexedRecord> mergedBeforeLate = merger.deltaMerge(patch, base).get();
    BufferedRecord<IndexedRecord> allLogs = merger.deltaMerge(late, mergedBeforeLate).get();
    BufferedRecord<IndexedRecord> logsOverBase = merger.finalMerge(base, merger.deltaMerge(late, patch).get());
    assertEquals("preserve", allLogs.getRecord().get(1).toString());
    assertEquals(allLogs.getRecord().get(1), logsOverBase.getRecord().get(1),
        "Moving the first record into a base file must not change the result of the same updates");
  }

  private static BufferedRecord<IndexedRecord> record(HoodieSchema schema, AvroRecordContext context, long timestamp, String value) {
    GenericData.Record data = new GenericData.Record(schema.toAvroSchema());
    data.put("ts", timestamp);
    data.put("value", value);
    return new BufferedRecord<>("a", timestamp, data, context.encodeSchema(schema), null);
  }
}
