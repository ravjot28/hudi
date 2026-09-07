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

package org.apache.hudi.table.action.bootstrap;

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.bootstrap.BootstrapWriteStatus;
import org.apache.hudi.client.bootstrap.selector.BootstrapModeSelector;
import org.apache.hudi.client.bootstrap.translator.BootstrapPartitionPathTranslator;
import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieAvroFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.execution.JavaLazyInsertIterable;
import org.apache.hudi.io.HoodieBootstrapHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BaseJavaCommitActionExecutor;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Bootstraps external files one at a time using the Java reader and the shared bootstrap index. */
public class JavaBootstrapCommitActionExecutor {
  private final HoodieEngineContext context;
  private final HoodieWriteConfig config;
  private final HoodieTable table;
  private final Option<Map<String, String>> extraMetadata;

  public JavaBootstrapCommitActionExecutor(HoodieEngineContext context, HoodieWriteConfig config,
                                           HoodieTable table, Option<Map<String, String>> extraMetadata) {
    this.context = context;
    this.config = config;
    this.table = table;
    this.extraMetadata = extraMetadata;
  }

  public HoodieBootstrapWriteMetadata<List<WriteStatus>> execute() {
    ValidationUtils.checkArgument(config.getBootstrapSourceBasePath() != null, "Bootstrap source path is required");
    ValidationUtils.checkArgument(table.getMetaClient().reloadActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().empty(), "Rollback bootstrap before bootstrapping a nonempty table");
    try {
      HoodieStorage sourceStorage = HoodieStorageUtils.getStorage(config.getBootstrapSourceBasePath(), table.getStorageConf());
      List<Pair<String, List<HoodieFileStatus>>> folders = BootstrapUtils.getAllLeafFoldersWithFiles(
          table.getBaseFileFormat(), sourceStorage, config.getBootstrapSourceBasePath(), context);
      Map<String, List<HoodieFileStatus>> files = folders.stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
      BootstrapModeSelector selector = (BootstrapModeSelector) ReflectionUtils.loadClass(config.getBootstrapModeSelectorClass(), config);
      Map<BootstrapMode, List<String>> selections = selector.select(folders);
      List<String> selected = selections.values().stream().flatMap(List::stream).collect(Collectors.toList());
      ValidationUtils.checkArgument(selected.size() == files.size() && files.keySet().equals(new HashSet<>(selected)),
          "Bootstrap selector must select each source partition exactly once");
      if (!selections.getOrDefault(BootstrapMode.FULL_RECORD, Collections.emptyList()).isEmpty()
          && !HoodieBootstrapConfig.FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME.defaultValue().equals(config.getFullBootstrapInputProvider())) {
        throw new UnsupportedOperationException("Java bootstrap currently reads external files directly; custom full-bootstrap providers are not supported");
      }
      HoodieSchema schema = resolveSchema(files);
      Option<HoodieWriteMetadata<List<WriteStatus>>> metadata = run(BootstrapMode.METADATA_ONLY, selections, files, schema);
      Option<HoodieWriteMetadata<List<WriteStatus>>> full = run(BootstrapMode.FULL_RECORD, selections, files, schema);
      return new HoodieBootstrapWriteMetadata<>(metadata, full);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to bootstrap " + config.getBasePath(), e);
    }
  }

  private HoodieSchema resolveSchema(Map<String, List<HoodieFileStatus>> files) throws IOException {
    if (config.getSchema() != null && !HoodieSchema.NULL_SCHEMA.equals(HoodieSchema.parse(config.getSchema()))) {
      return HoodieSchema.parse(config.getSchema());
    }
    HoodieFileStatus first = files.values().stream().flatMap(List::stream).findFirst()
        .orElseThrow(() -> new HoodieException("Cannot infer bootstrap schema from an empty source"));
    try (HoodieAvroFileReader reader = openReader(first)) {
      return HoodieSchemaUtils.removeMetadataFields(reader.getSchema());
    }
  }

  private HoodieAvroFileReader openReader(HoodieFileStatus file) throws IOException {
    StoragePath path = new StoragePath(file.getPath().getUri());
    HoodieStorage storage = HoodieStorageUtils.getStorage(path, table.getStorageConf());
    return (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
        .getFileReader(config, path);
  }

  private Option<HoodieWriteMetadata<List<WriteStatus>>> run(BootstrapMode mode,
      Map<BootstrapMode, List<String>> selections, Map<String, List<HoodieFileStatus>> files, HoodieSchema schema) {
    List<String> partitions = selections.getOrDefault(mode, Collections.emptyList());
    if (partitions.isEmpty()) {
      return Option.empty();
    }
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProps(config.getProps())
        .withSchema(schema.toString()).withWriteStatusClass(BootstrapWriteStatus.class).build();
    return Option.of(new BootstrapExecutor(writeConfig, mode, partitions, files, schema).execute());
  }

  private class BootstrapExecutor extends BaseJavaCommitActionExecutor<IndexedRecord> {
    private final BootstrapMode mode;
    private final List<String> partitions;
    private final Map<String, List<HoodieFileStatus>> files;
    private final HoodieSchema schema;

    BootstrapExecutor(HoodieWriteConfig writeConfig, BootstrapMode mode, List<String> partitions,
                      Map<String, List<HoodieFileStatus>> files, HoodieSchema schema) {
      super(JavaBootstrapCommitActionExecutor.this.context, writeConfig, JavaBootstrapCommitActionExecutor.this.table,
          mode == BootstrapMode.METADATA_ONLY ? HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS : HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
          WriteOperationType.BOOTSTRAP, JavaBootstrapCommitActionExecutor.this.extraMetadata);
      this.mode = mode;
      this.partitions = partitions;
      this.files = files;
      this.schema = schema;
    }

    @Override
    public HoodieWriteMetadata<List<WriteStatus>> execute() {
      HoodieInstant requested = instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, getCommitActionType(), instantTime);
      table.getActiveTimeline().createNewInstant(requested);
      table.getActiveTimeline().transitionRequestedToInflight(requested, Option.empty());
      BaseKeyGenerator keyGenerator;
      try {
        keyGenerator = (BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(config.getProps());
      } catch (IOException e) {
        throw new HoodieIOException("Cannot create bootstrap key generator", e);
      }
      BootstrapPartitionPathTranslator translator = ReflectionUtils.loadClass(config.getBootstrapPartitionPathTranslatorClass());
      List<WriteStatus> statuses = new ArrayList<>();
      for (String sourcePartition : partitions) {
        String partition = translator.getBootstrapTranslatedPath(sourcePartition);
        for (HoodieFileStatus file : files.get(sourcePartition)) {
          try (HoodieAvroFileReader reader = openReader(file);
               ClosableIterator<IndexedRecord> records = sourceRecords(reader, keyGenerator)) {
            if (mode == BootstrapMode.METADATA_ONLY) {
              HoodieBootstrapHandle handle = new HoodieBootstrapHandle(config, instantTime, table, partition,
                  FSUtils.createNewFileIdPfx(), taskContextSupplier);
              try {
                while (records.hasNext()) {
                  String key = keyGenerator.getRecordKey((GenericRecord) records.next());
                  GenericRecord skeleton = new GenericData.Record(HoodieBootstrapHandle.METADATA_BOOTSTRAP_RECORD_SCHEMA.toAvroSchema());
                  skeleton.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, key);
                  handle.write(new HoodieAvroIndexedRecord(new HoodieKey(key, partition), skeleton),
                      handle.getWriterSchema(), config.getProps());
                }
              } finally {
                handle.close();
              }
              BootstrapWriteStatus status = (BootstrapWriteStatus) handle.getWriteStatuses().get(0);
              status.setBootstrapSourceFileMapping(new BootstrapFileMapping(config.getBootstrapSourceBasePath(),
                  sourcePartition, file, partition, status.getFileId()));
              statuses.add(status);
            } else {
              ClosableIterator<HoodieRecord<IndexedRecord>> input = new CloseableMappingIterator<>(records,
                  record -> (HoodieRecord) new HoodieAvroIndexedRecord(
                      new HoodieKey(keyGenerator.getRecordKey((GenericRecord) record), partition), record));
              new JavaLazyInsertIterable<>(input, false, config, instantTime, table, FSUtils.createNewFileIdPfx(),
                  taskContextSupplier).forEachRemaining(statuses::addAll);
            }
          } catch (IOException e) {
            throw new HoodieIOException("Failed to bootstrap " + file.getPath().getUri(), e);
          }
        }
      }
      if (statuses.stream().anyMatch(status -> status.hasErrors() || status.hasGlobalError())) {
        throw new HoodieException("Bootstrap write failed; rollback before retrying");
      }
      HoodieWriteMetadata<List<WriteStatus>> result = new HoodieWriteMetadata<>();
      updateIndexAndMaybeRunPreCommitValidations(statuses, result);
      completeCommit(result);
      WriteMarkersFactory.get(config.getMarkersType(), table, instantTime)
          .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
      return result;
    }

    private ClosableIterator<IndexedRecord> sourceRecords(HoodieAvroFileReader reader, BaseKeyGenerator keyGenerator) throws IOException {
      HoodieSchema sourceSchema = reader.getSchema();
      HoodieSchema requestedSchema = mode == BootstrapMode.METADATA_ONLY
          ? HoodieSchemaUtils.generateProjectionSchema(sourceSchema, keyGenerator.getRecordKeyFieldNames().stream()
              .map(HoodieAvroUtils::getRootLevelFieldName).distinct().collect(Collectors.toList()))
          : sourceSchema;
      return reader.getIndexedRecordIterator(sourceSchema, requestedSchema);
    }

    @Override
    protected void commit(HoodieWriteMetadata<List<WriteStatus>> result) {
      if (mode == BootstrapMode.METADATA_ONLY) {
        Map<String, List<BootstrapFileMapping>> mappings = new HashMap<>();
        for (WriteStatus status : result.getWriteStatuses()) {
          BootstrapFileMapping mapping = ((BootstrapWriteStatus) status).getBootstrapSourceFileMapping();
          mappings.computeIfAbsent(mapping.getPartitionPath(), ignored -> new ArrayList<>()).add(mapping);
        }
        try (BootstrapIndex.IndexWriter writer = BootstrapIndex.getBootstrapIndex(table.getMetaClient())
            .createWriter(config.getBootstrapSourceBasePath())) {
          writer.begin();
          mappings.forEach(writer::appendNextPartition);
          writer.finish();
        }
      }
      super.commit(result);
    }

    @Override
    protected String getSchemaToStoreInCommit() {
      return schema.toString();
    }
  }
}
