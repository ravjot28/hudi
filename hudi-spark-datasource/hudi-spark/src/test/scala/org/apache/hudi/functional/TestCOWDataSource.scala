/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.DataSourceWriteOptions.{INLINE_CLUSTERING_ENABLE, KEYGENERATOR_CLASS_NAME}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.QuickstartUtils.{convertToStringList, getQuickstartWriteConfigs}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig.{TIMESTAMP_INPUT_DATE_FORMAT, TIMESTAMP_OUTPUT_DATE_FORMAT, TIMESTAMP_TIMEZONE_FORMAT, TIMESTAMP_TYPE_FIELD}
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline, TimelineUtils}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.{deleteRecordsToStrings, recordsToStrings}
import org.apache.hudi.common.util
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.metrics.HoodieMetricsConfig
import org.apache.hudi.exception.ExceptionUtil.getRootCause
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.functional.CommonOptionUtils._
import org.apache.hudi.functional.TestCOWDataSource.convertColumnsToNullable
import org.apache.hudi.hive.HiveSyncConfigHolder
import org.apache.hudi.keygen._
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.metrics.{Metrics, MetricsReporterType}
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers, QuickstartUtils, ScalaAssertionSupport}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{AfterEach, BeforeEach, Disabled, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}

import java.sql.{Date, Timestamp}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.function.Consumer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.matching.Regex


/**
 * Basic tests on the spark datasource for COW table.
 */
class TestCOWDataSource extends HoodieSparkClientTestBase with ScalaAssertionSupport {
  var spark: SparkSession = null

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  override def getSparkSessionExtensionsInjector: util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
    FileSystem.closeAll()
    System.gc()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testShortNameStorage(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testNoPrecombine(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    ) ++ writeOpts
    inputDF.write.format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()
  }

  @Test
  def testInferPartitionBy(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO, Map())
    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    ) ++ writeOpts

    inputDF.write.partitionBy("partition").format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshot0 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    snapshot0.cache()
    assertEquals(100, snapshot0.count())

    // triggering 2nd batch to ensure table config validation does not fail.
    inputDF.write.partitionBy("partition").format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // verify partition cols
    assertTrue(snapshot0.filter("_hoodie_partition_path = '" + HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "'").count() > 0)
    assertTrue(snapshot0.filter("_hoodie_partition_path = '" + HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH + "'").count() > 0)
    assertTrue(snapshot0.filter("_hoodie_partition_path = '" + HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH + "'").count() > 0)
    val fs = new Path(basePath).getFileSystem(new Configuration())
    assertTrue(fs.exists(new Path(basePath + "/" + HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)))
    assertTrue(fs.exists(new Path(basePath + "/" + HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)))
    assertTrue(fs.exists(new Path(basePath + "/" + HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)))

    // try w/ multi field partition paths
    // generate two batches of df w/ diff partition path values.
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    val records2 = recordsToStrings(dataGen.generateInserts("000", 200)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    // hard code the value for rider and fare so that we can verify the partitions paths with hudi
    val toInsertDf = inputDF1.withColumn("fare", lit(100)).withColumn("rider", lit("rider-123"))
      .union(inputDF2.withColumn("fare", lit(200)).withColumn("rider", lit("rider-456")))

    toInsertDf.write.partitionBy("fare", "rider").format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshot1 = spark.read.format("org.apache.hudi").options(readOpts).load(basePath)
    snapshot1.cache()
    assertEquals(300, snapshot1.count())

    var partitionPaths = FSUtils.getAllPartitionPaths(new HoodieSparkEngineContext(jsc), HoodieMetadataConfig.newBuilder().build(), basePath)
    assertTrue(partitionPaths.contains("100/rider-123"))
    assertTrue(partitionPaths.contains("200/rider-456"))

    // verify partition cols
    assertEquals(snapshot1.filter("_hoodie_partition_path = '100/rider-123'").count(), 100)
    assertEquals(snapshot1.filter("_hoodie_partition_path = '200/rider-456'").count(), 200)

    // triggering 2nd batch to ensure table config validation does not fail.
    toInsertDf.write.partitionBy("fare", "rider").format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // incase of non partitioned dataset, inference should not happen.
    toInsertDf.write.partitionBy("fare", "rider").format("hudi")
      .options(commonOptsNoPreCombine)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(KEYGENERATOR_CLASS_NAME.key(), classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    partitionPaths = FSUtils.getAllPartitionPaths(new HoodieSparkEngineContext(jsc), HoodieMetadataConfig.newBuilder().build(), basePath)
    assertEquals(partitionPaths.length, 1)
    assertEquals(partitionPaths.get(0), "")
  }

  @Test
  def testReuseTableConfigs() {
    val recordType = HoodieRecordType.AVRO
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      HoodieMetadataConfig.ENABLE.key -> "false" // this is testing table configs and write configs. disabling metadata to save on test run time.
    ))

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.ENABLE.key -> "false"
    ) ++ writeOpts

    writeToHudi(commonOptsNoPreCombine, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()

    val optsWithNoRepeatedTableConfig = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieMetadataConfig.ENABLE.key -> "false"
    ) ++ writeOpts
    // this write should succeed even w/o setting any param for record key, partition path since table config will be re-used.
    writeToHudi(optsWithNoRepeatedTableConfig, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()
    assertLastCommitIsUpsert()
  }

  @Test
  def testSimpleKeyGenDroppingConfigs() {
    val recordType = HoodieRecordType.AVRO
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      HoodieMetadataConfig.ENABLE.key -> "false" // this is testing table configs and write configs. disabling metadata to save on test run time.
    ))

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.ENABLE.key -> "false"
    ) ++ writeOpts

    writeToHudi(commonOptsNoPreCombine, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()

    val optsWithNoRepeatedTableConfig = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieMetadataConfig.ENABLE.key -> "false"
    )
    // this write should succeed even w/o though we don't set key gen explicitly.
    writeToHudi(optsWithNoRepeatedTableConfig, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()
    assertLastCommitIsUpsert()
  }

  @Test
  def testSimpleKeyGenExtraneuousAddition() {
    val recordType = HoodieRecordType.AVRO
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1"
    ))

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.ENABLE.key -> "false" // this is testing table configs and write configs. disabling metadata to save on test run time.
    ) ++ writeOpts

    writeToHudi(commonOptsNoPreCombine, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()

    val optsWithNoRepeatedTableConfig = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieMetadataConfig.ENABLE.key -> "false"
    )
    // this write should succeed even w/o though we set key gen explicitly, its the default
    writeToHudi(optsWithNoRepeatedTableConfig, inputDF)
    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()
    assertLastCommitIsUpsert()
  }

  private def writeToHudi(opts: Map[String, String], df: Dataset[Row]): Unit = {
    df.write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
  }

  @ParameterizedTest
  @CsvSource(Array("hoodie.datasource.write.recordkey.field,begin_lat", "hoodie.datasource.write.partitionpath.field,end_lon",
    "hoodie.datasource.write.keygenerator.class,org.apache.hudi.keygen.NonpartitionedKeyGenerator", "hoodie.datasource.write.precombine.field,fare"))
  def testAlteringRecordKeyConfig(configKey: String, configValue: String) {
    val recordType = HoodieRecordType.AVRO
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      "hoodie.datasource.write.precombine.field" -> "ts",
      HoodieMetadataConfig.ENABLE.key -> "false" // this is testing table configs and write configs. disabling metadata to save on test run time.
    ))

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val commonOptsNoPreCombine = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.ENABLE.key -> "false"
    ) ++ writeOpts
    writeToHudi(commonOptsNoPreCombine, inputDF)

    spark.read.format("org.apache.hudi").options(readOpts).load(basePath).count()

    val optsForBatch2 = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieMetadataConfig.ENABLE.key -> "false",
      configKey -> configValue
    )

    // this write should fail since we are setting a config explicitly which wasn't set in first commit and does not match the default value.
    val t = assertThrows(classOf[Throwable]) {
      writeToHudi(optsForBatch2, inputDF)
    }
    assertTrue(getRootCause(t).getMessage.contains("Config conflict"))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testHoodieIsDeletedNonBooleanField(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    val df = inputDF.withColumn(HoodieRecord.HOODIE_IS_DELETED_FIELD, lit("abc"))

    // Should have failed since _hoodie_is_deleted is not a BOOLEAN data type
    assertThrows(classOf[HoodieException]) {
      df.write.format("hudi")
        .options(writeOpts)
        .mode(SaveMode.Overwrite)
        .save(basePath)
    }
  }

  /**
   * This tests the case that query by with a specified partition condition on hudi table which is
   * different between the value of the partition field and the actual partition path,
   * like hudi table written by TimestampBasedKeyGenerator.
   *
   * For COW table, test the snapshot query mode and incremental query mode.
   */
  @ParameterizedTest
  @CsvSource(Array("true,AVRO", "true,SPARK", "false,AVRO", "false,SPARK"))
  def testPrunePartitionForTimestampBasedKeyGenerator(enableFileIndex: Boolean,
                                                      recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, enableFileIndex = enableFileIndex)

    val options = commonOpts ++ Map(
      "hoodie.compact.inline" -> "false",
      DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
      TIMESTAMP_TYPE_FIELD.key -> "DATE_STRING",
      TIMESTAMP_OUTPUT_DATE_FORMAT.key -> "yyyy/MM/dd",
      TIMESTAMP_TIMEZONE_FORMAT.key -> "GMT+8:00",
      TIMESTAMP_INPUT_DATE_FORMAT.key -> "yyyy-MM-dd"
    ) ++ writeOpts

    val dataGen1 = new HoodieTestDataGenerator(Array("2022-01-01"))
    val records1 = recordsToStrings(dataGen1.generateInserts("001", 20)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf)
      .build()
    val commit1Time = metaClient.getActiveTimeline.lastInstant().get().getTimestamp

    val dataGen2 = new HoodieTestDataGenerator(Array("2022-01-02"))
    val records2 = recordsToStrings(dataGen2.generateInserts("002", 30)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit2Time = metaClient.reloadActiveTimeline.lastInstant().get().getTimestamp

    // snapshot query
    val pathForReader = getPathForReader(basePath, !enableFileIndex, 3)
    val snapshotQueryRes = spark.read.format("hudi").options(readOpts).load(pathForReader)
    // TODO(HUDI-3204) we have to revert this to pre-existing behavior from 0.10
    if (enableFileIndex) {
      assertEquals(snapshotQueryRes.where("partition = '2022/01/01'").count, 20)
      assertEquals(snapshotQueryRes.where("partition = '2022/01/02'").count, 30)
    } else {
      assertEquals(snapshotQueryRes.where("partition = '2022-01-01'").count, 20)
      assertEquals(snapshotQueryRes.where("partition = '2022-01-02'").count, 30)
    }

    // incremental query
    val incrementalQueryRes = spark.read.format("hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commit1Time)
      .option(DataSourceReadOptions.END_INSTANTTIME.key, commit2Time)
      .load(basePath)
    assertEquals(incrementalQueryRes.where("partition = '2022-01-01'").count, 0)
    assertEquals(incrementalQueryRes.where("partition = '2022-01-02'").count, 30)
  }

  /**
   * Test for https://issues.apache.org/jira/browse/HUDI-1615. Null Schema in BulkInsert row writer flow.
   * This was reported by customer when archival kicks in as the schema in commit metadata is not set for bulk_insert
   * row writer flow.
   * In this test, we trigger a round of bulk_inserts and set archive related configs to be minimal. So, after 6 rounds,
   * archival should kick in and 2 commits should be archived. If schema is valid, no exception will be thrown. If not,
   * NPE will be thrown.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testArchivalWithBulkInsert(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    var structType: StructType = null
    for (i <- 1 to 7) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), 100)).toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      structType = inputDF.schema
      inputDF.write.format("hudi")
        .options(writeOpts)
        .option("hoodie.keep.min.commits", "4")
        .option("hoodie.keep.max.commits", "5")
        .option("hoodie.cleaner.commits.retained", "0")
        .option("hoodie.datasource.write.row.writer.enable", "true")
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
        .mode(if (i == 0) SaveMode.Overwrite else SaveMode.Append)
        .save(basePath)
    }

    val tableMetaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath).build()
    assertFalse(tableMetaClient.getArchivedTimeline.empty())

    val actualSchema = new TableSchemaResolver(tableMetaClient).getTableAvroSchema(false)
    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(commonOpts(HoodieWriteConfig.TBL_NAME.key))
    spark.sparkContext.getConf.registerKryoClasses(
      Array(classOf[org.apache.avro.generic.GenericData],
        classOf[org.apache.avro.Schema]))
    val schema = AvroConversionUtils.convertStructTypeToAvroSchema(structType, structName, nameSpace)
    assertTrue(actualSchema != null)
    assertEquals(schema, actualSchema)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testCopyOnWriteDeletes(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, snapshotDF1.count())

    val records2 = deleteRecordsToStrings(dataGen.generateUniqueDeletes(20)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))

    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*/*")
    assertEquals(snapshotDF2.count(), 80)
  }

  /**
   * Test retries on conflict failures.
   */
  @ParameterizedTest
  @ValueSource(ints = Array(0, 2))
  def testCopyOnWriteConcurrentUpdates(numRetries: Integer): Unit = {
    initTestDataGenerator()
    val records1 = recordsToStrings(dataGen.generateInserts("000", 1000)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
      .option("hoodie.cleaner.policy.failed.writes", "LAZY")
      .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.InProcessLockProvider")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    assertEquals(1000, snapshotDF1.count())

    val countDownLatch = new CountDownLatch(2)
    for (x <- 1 to 2) {
      val thread = new Thread(new UpdateThread(dataGen, spark, commonOpts, basePath, x + "00", countDownLatch, numRetries))
      thread.setName((x + "00_THREAD").toString())
      thread.start()
    }
    countDownLatch.await(1, TimeUnit.MINUTES)

    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    if (numRetries > 0) {
      assertEquals(snapshotDF2.count(), 3000)
      assertEquals(HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size(), 3)
    } else {
      // only one among two threads will succeed and hence 2000
      assertEquals(snapshotDF2.count(), 2000)
      assertEquals(HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size(), 2)
    }
  }

  class UpdateThread(dataGen: HoodieTestDataGenerator, spark: SparkSession, commonOpts: Map[String, String], basePath: String,
                     instantTime: String, countDownLatch: CountDownLatch, numRetries: Integer = 0) extends Runnable {
    override def run() {
      val updateRecs = recordsToStrings(dataGen.generateUniqueUpdates(instantTime, 500)).toList
      val insertRecs = recordsToStrings(dataGen.generateInserts(instantTime, 1000)).toList
      val updateDf = spark.read.json(spark.sparkContext.parallelize(updateRecs, 2))
      val insertDf = spark.read.json(spark.sparkContext.parallelize(insertRecs, 2))
      updateDf.union(insertDf).write.format("org.apache.hudi")
        .options(commonOpts)
        .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
        .option("hoodie.cleaner.policy.failed.writes", "LAZY")
        .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.InProcessLockProvider")
        .option(HoodieWriteConfig.NUM_RETRIES_ON_CONFLICT_FAILURES.key(), numRetries.toString)
        .mode(SaveMode.Append)
        .save(basePath)
      countDownLatch.countDown()
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testOverWriteModeUseReplaceAction(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 5)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
      .setLoadActiveTimelineOnLoad(true).build();
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => (instant.asInstanceOf[HoodieInstant]).getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testReadPathsOnCopyOnWriteTable(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val records1 = dataGen.generateInsertsContainsAllPartitions("001", 20)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
      .setLoadActiveTimelineOnLoad(true).build()

    val instantTime = metaClient.getActiveTimeline.filterCompletedInstants().getInstantsAsStream.findFirst().get().getTimestamp

    val record1FilePaths = fs.listStatus(new Path(basePath, dataGen.getPartitionPaths.head))
      .filter(!_.getPath.getName.contains("hoodie_partition_metadata"))
      .filter(_.getPath.getName.endsWith("parquet"))
      .map(_.getPath.toString)
      .mkString(",")

    val records2 = dataGen.generateInsertsContainsAllPartitions("002", 20)
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2), 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val inputDF3 = spark.read.options(readOpts).json(spark.sparkContext.parallelize(recordsToStrings(records2), 2))
    inputDF3.write.format("org.apache.hudi")
      .options(writeOpts)
      // Use bulk insert here to make sure the files have different file groups.
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val hudiReadPathDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key(), instantTime)
      .option(DataSourceReadOptions.READ_PATHS.key, record1FilePaths)
      .load()

    val expectedCount = records1.asScala.count(record => record.getPartitionPath == dataGen.getPartitionPaths.head)
    assertEquals(expectedCount, hudiReadPathDF.count())
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testOverWriteTableModeUseReplaceAction(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 5)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
      .setLoadActiveTimelineOnLoad(true).build()
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => (instant.asInstanceOf[HoodieInstant]).getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testOverWriteModeUseReplaceActionOnDisJointPartitions(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // step1: Write 5 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("001", 5, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step2: Write 7 records to hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("002", 7, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step3: Write 6 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH using INSERT_OVERWRITE_OPERATION_OPT_VAL
    val records3 = recordsToStrings(dataGen.generateInsertsForPartition("001", 6, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val allRecords = spark.read.format("org.apache.hudi").options(readOpts).load(basePath + "/*/*/*")
    allRecords.registerTempTable("tmpTable")

    spark.sql(String.format("select count(*) from tmpTable")).show()

    // step4: Query the rows count from hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val recordCountForPartition1 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).collect()
    assertEquals("6", recordCountForPartition1(0).get(0).toString)

    // step5: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH
    val recordCountForPartition2 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).collect()
    assertEquals("7", recordCountForPartition2(0).get(0).toString)

    // step6: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH using spark.collect and then filter mode
    val recordsForPartitionColumn = spark.sql(String.format("select partition from tmpTable")).collect()
    val filterSecondPartitionCount = recordsForPartitionColumn.filter(row => row.get(0).equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).size
    assertEquals(7, filterSecondPartitionCount)

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
      .setLoadActiveTimelineOnLoad(true).build()
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => instant.asInstanceOf[HoodieInstant].getAction)
    assertEquals(3, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("commit", commits(1))
    assertEquals("replacecommit", commits(2))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testOverWriteTableModeUseReplaceActionOnDisJointPartitions(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    // step1: Write 5 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("001", 5, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step2: Write 7 more records using SaveMode.Overwrite for partition2 DEFAULT_SECOND_PARTITION_PATH
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("002", 7, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val allRecords = spark.read.format("org.apache.hudi").options(readOpts).load(basePath + "/*/*/*")
    allRecords.registerTempTable("tmpTable")

    spark.sql(String.format("select count(*) from tmpTable")).show()

    // step3: Query the rows count from hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val recordCountForPartition1 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).collect()
    assertEquals("0", recordCountForPartition1(0).get(0).toString)

    // step4: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH
    val recordCountForPartition2 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).collect()
    assertEquals("7", recordCountForPartition2(0).get(0).toString)

    // step5: Query the rows count from hoodie table
    val recordCount = spark.sql(String.format("select count(*) from tmpTable")).collect()
    assertEquals("7", recordCount(0).get(0).toString)

    // step6: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH using spark.collect and then filter mode
    val recordsForPartitionColumn = spark.sql(String.format("select partition from tmpTable")).collect()
    val filterSecondPartitionCount = recordsForPartitionColumn.filter(row => row.get(0).equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).size
    assertEquals(7, filterSecondPartitionCount)

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
      .setLoadActiveTimelineOnLoad(true).build()
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => instant.asInstanceOf[HoodieInstant].getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testDropInsertDup(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val insert1Cnt = 10
    val insert2DupKeyCnt = 9
    val insert2NewKeyCnt = 2

    val totalUniqueKeyToGenerate = insert1Cnt + insert2NewKeyCnt
    val allRecords = dataGen.generateInserts("001", totalUniqueKeyToGenerate)
    val inserts1 = allRecords.subList(0, insert1Cnt)
    val inserts2New = dataGen.generateSameKeyInserts("002", allRecords.subList(insert1Cnt, insert1Cnt + insert2NewKeyCnt))
    val inserts2Dup = dataGen.generateSameKeyInserts("002", inserts1.subList(0, insert2DupKeyCnt))

    val records1 = recordsToStrings(inserts1).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*/*")
    assertEquals(insert1Cnt, hoodieROViewDF1.count())

    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val records2 = recordsToStrings(inserts2Dup ++ inserts2New).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.INSERT_DROP_DUPS.key, "true")
      .mode(SaveMode.Append)
      .save(basePath)
    val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*/*")
    assertEquals(hoodieROViewDF2.count(), totalUniqueKeyToGenerate)

    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime1)
      .load(basePath)
    assertEquals(hoodieIncViewDF2.count(), insert2NewKeyCnt)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testComplexDataTypeWriteAndReadConsistency(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val schema = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, true)
      :: StructField("timeStampValue", TimestampType, true) :: StructField("dateValue", DateType, true)
      :: StructField("decimalValue", DataTypes.createDecimalType(15, 10), true) :: StructField("timestamp", IntegerType, true)
      :: StructField("partition", IntegerType, true) :: Nil)

    val records = Seq(Row("11", "Andy", Timestamp.valueOf("1970-01-01 13:31:24"), Date.valueOf("1991-11-07"), BigDecimal.valueOf(1.0), 11, 1),
      Row("22", "lisi", Timestamp.valueOf("1970-01-02 13:31:24"), Date.valueOf("1991-11-08"), BigDecimal.valueOf(2.0), 11, 1),
      Row("33", "zhangsan", Timestamp.valueOf("1970-01-03 13:31:24"), Date.valueOf("1991-11-09"), BigDecimal.valueOf(3.0), 11, 1))
    val rdd = jsc.parallelize(records)
    val recordsDF = spark.createDataFrame(rdd, schema)
    recordsDF.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*")
    recordsReadDF.printSchema()
    recordsReadDF.schema.foreach(f => {
      f.name match {
        case "timeStampValue" =>
          assertEquals(f.dataType, org.apache.spark.sql.types.TimestampType)
        case "dateValue" =>
          assertEquals(f.dataType, org.apache.spark.sql.types.DateType)
        case "decimalValue" =>
          assertEquals(f.dataType, org.apache.spark.sql.types.DecimalType(15, 10))
        case _ =>
      }
    })
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testWithAutoCommitOn(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.AUTO_COMMIT_ENABLE.key, "true")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
  }

  private def getDataFrameWriter(keyGenerator: String, opts: Map[String, String]): DataFrameWriter[Row] = {
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, keyGenerator)
      .mode(SaveMode.Overwrite)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithCustomKeyGeneratorWithGlobbing(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)

    // Without fieldType, the default is SIMPLE
    var writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*")

    assertEquals(0L, recordsReadDF.filter(col("_hoodie_partition_path") =!= col("current_ts").cast("string")).count())

    // Specify fieldType as TIMESTAMP
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts:TIMESTAMP")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyyMMdd")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*")
    val udf_date_format = udf((data: Long) => new DateTime(data).toString(DateTimeFormat.forPattern("yyyyMMdd")))

    assertEquals(0L, recordsReadDF.filter(col("_hoodie_partition_path") =!= udf_date_format(col("current_ts"))).count())

    // Mixed fieldType
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver", "rider:SIMPLE", "current_ts:TIMESTAMP")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyyMMdd")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*/*")
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!=
      concat(col("driver"), lit("/"), col("rider"), lit("/"), udf_date_format(col("current_ts")))).count() == 0)

    // Test invalid partitionKeyType
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer = writer.partitionBy("current_ts:DUMMY")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyyMMdd")
    try {
      writer.save(basePath)
      fail("should fail when invalid PartitionKeyType is provided!")
    } catch {
      case e: Exception =>
        assertTrue(e.getCause.getMessage.contains("No enum constant org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType.DUMMY"))
    }
  }

  @Disabled("HUDI-6320")
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithCustomKeyGenerator(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)
    // Specify fieldType as TIMESTAMP of type EPOCHMILLISECONDS and output date format as yyyy/MM/dd
    var writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts:TIMESTAMP")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy/MM/dd")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
    val udf_date_format = udf((data: Long) => new DateTime(data).toString(DateTimeFormat.forPattern("yyyy/MM/dd")))

    assertEquals(0L, recordsReadDF.filter(col("_hoodie_partition_path") =!= udf_date_format(col("current_ts"))).count())

    // Mixed fieldType with TIMESTAMP of type EPOCHMILLISECONDS and output date format as yyyy/MM/dd
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver", "rider:SIMPLE", "current_ts:TIMESTAMP")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy/MM/dd")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    recordsReadDF = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!=
      concat(col("driver"), lit("/"), col("rider"), lit("/"), udf_date_format(col("current_ts")))).count() == 0)
  }

  @Test
  def testPartitionPruningForTimestampBasedKeyGenerator(): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(HoodieRecordType.AVRO, enableFileIndex = true)
    val writer = getDataFrameWriter(classOf[TimestampBasedKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy/MM/dd")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshotQueryRes = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
      .where("current_ts > '1970/01/16'")
    assertTrue(checkPartitionFilters(snapshotQueryRes.queryExecution.executedPlan.toString, "current_ts.* > 1970/01/16"))
  }

  def checkPartitionFilters(sparkPlan: String, partitionFilter: String): Boolean = {
    val partitionFilterPattern: Regex = """PartitionFilters: \[(.*?)\]""".r
    val tsPattern: Regex = (partitionFilter).r

    val partitionFilterMatch = partitionFilterPattern.findFirstMatchIn(sparkPlan)

    partitionFilterMatch match {
      case Some(m) =>
        val filters = m.group(1)
        tsPattern.findFirstIn(filters).isDefined
      case None =>
        false
    }
  }

  @Test
  def testSparkPartitionByWithSimpleKeyGenerator() {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(HoodieRecordType.AVRO)
    // Use the `driver` field as the partition key
    var writer = getDataFrameWriter(classOf[SimpleKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= col("driver")).count() == 0)

    // Use the `driver,rider` field as the partition key, If no such field exists,
    // the default value [[PartitionPathEncodeUtils#DEFAULT_PARTITION_PATH]] is used
    writer = getDataFrameWriter(classOf[SimpleKeyGenerator].getName, writeOpts)
    val t = assertThrows(classOf[Throwable]) {
      writer.partitionBy("driver", "rider")
        .save(basePath)
    }

    assertEquals("Single partition-path field is expected; provided (driver,rider)", getRootCause(t).getMessage)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithComplexKeyGenerator(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)
    // Use the `driver` field as the partition key
    var writer = getDataFrameWriter(classOf[ComplexKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= col("driver")).count() == 0)

    // Use the `driver`,`rider` field as the partition key
    writer = getDataFrameWriter(classOf[ComplexKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver", "rider")
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= concat(col("driver"), lit("/"), col("rider"))).count() == 0)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithTimestampBasedKeyGenerator(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)

    val writer = getDataFrameWriter(classOf[TimestampBasedKeyGenerator].getName, writeOpts)
    writer.partitionBy("current_ts")
      .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyyMMdd")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath + "/*/*")
    val udf_date_format = udf((data: Long) => new DateTime(data).toString(DateTimeFormat.forPattern("yyyyMMdd")))
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= udf_date_format(col("current_ts"))).count() == 0)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithGlobalDeleteKeyGenerator(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)
    val writer = getDataFrameWriter(classOf[GlobalDeleteKeyGenerator].getName, writeOpts)
    writer.partitionBy("driver")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit("")).count() == 0)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSparkPartitionByWithNonpartitionedKeyGenerator(recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOptsLessPartitionPath(recordType)
    // Empty string column
    var writer = getDataFrameWriter(classOf[NonpartitionedKeyGenerator].getName, writeOpts)
    writer.partitionBy("")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    var recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit("")).count() == 0)

    // Non-existent column
    writer = getDataFrameWriter(classOf[NonpartitionedKeyGenerator].getName, writeOpts)
    writer.partitionBy("abc")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    recordsReadDF = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit("")).count() == 0)
  }

  private def testPartitionPruning(enableFileIndex: Boolean,
                                   partitionEncode: Boolean,
                                   isMetadataEnabled: Boolean,
                                   recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, enableFileIndex = enableFileIndex)

    val N = 20
    // Test query with partition prune if URL_ENCODE_PARTITIONING has enable
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", N)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    val countIn20160315 = records1.asScala.count(record => record.getPartitionPath == "2016/03/15")
    val pathForReader = getPathForReader(basePath, !enableFileIndex, if (partitionEncode) 1 else 3)
    // query the partition by filter
    val count1 = spark.read.format("hudi")
      .options(readOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(pathForReader)
      .filter("partition = '2016/03/15'")
      .count()
    assertEquals(countIn20160315, count1)

    // query the partition by path
    val partitionPath = if (partitionEncode) "2016%2F03%2F15" else "2016/03/15"
    val count2 = spark.read.format("hudi")
      .options(readOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath + s"/$partitionPath")
      .count()
    assertEquals(countIn20160315, count2)

    // Second write with Append mode
    val records2 = dataGen.generateInsertsContainsAllPartitions("000", N + 1)
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2), 2))
    inputDF2.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, partitionEncode)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)
    // Incremental query without "*" in path
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, commitInstantTime1)
      .load(basePath)
    assertEquals(N + 1, hoodieIncViewDF1.count())
    assertEquals(false, Metrics.isInitialized(basePath))
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true,false,AVRO", "true,true,AVRO", "false,true,AVRO", "false,false,AVRO",
    "true,false,SPARK", "true,true,SPARK", "false,true,SPARK", "false,false,SPARK"
  ))
  def testQueryCOWWithBasePathAndFileIndex(partitionEncode: Boolean, isMetadataEnabled: Boolean, recordType: HoodieRecordType): Unit = {
    testPartitionPruning(
      enableFileIndex = true,
      partitionEncode = partitionEncode,
      isMetadataEnabled = isMetadataEnabled,
      recordType = recordType)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPartitionPruningWithoutFileIndex(partitionEncode: Boolean): Unit = {
    testPartitionPruning(
      enableFileIndex = false,
      partitionEncode = partitionEncode,
      isMetadataEnabled = HoodieMetadataConfig.ENABLE.defaultValue,
      recordType = HoodieRecordType.SPARK)
  }

  @Test def testSchemaNotEqualData(): Unit = {
    val opts = commonOpts ++ Map("hoodie.avro.schema.validate" -> "true")
    val schema1 = StructType(StructField("_row_key", StringType, nullable = true) :: StructField("name", StringType, nullable = true) ::
      StructField("timestamp", IntegerType, nullable = true) :: StructField("age", StringType, nullable = true) :: StructField("partition", IntegerType, nullable = true) :: Nil)
    val records = Array("{\"_row_key\":\"1\",\"name\":\"lisi\",\"timestamp\":1,\"partition\":1}",
      "{\"_row_key\":\"1\",\"name\":\"lisi\",\"timestamp\":1,\"partition\":1}")
    val inputDF = spark.read.schema(schema1.toDDL).json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath)
    val resultSchema = new StructType(recordsReadDF.schema.filter(p => !p.name.startsWith("_hoodie")).toArray)
    assertEquals(resultSchema, schema1)
  }

  @ParameterizedTest
  @CsvSource(Array("true, AVRO", "false, AVRO", "true, SPARK", "false, SPARK"))
  def testCopyOnWriteWithDroppedPartitionColumns(enableDropPartitionColumns: Boolean, recordType: HoodieRecordType) {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val records1 = recordsToStrings(dataGen.generateInsertsContainsAllPartitions("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.DROP_PARTITION_COLUMNS.key, enableDropPartitionColumns)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val snapshotDF1 = spark.read.format("hudi").options(readOpts).load(basePath)
    assertEquals(snapshotDF1.count(), 100)
    assertEquals(3, snapshotDF1.select("partition").distinct().count())
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testHoodieIsDeletedCOW(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)

    val numRecords = 100
    val numRecordsToDelete = 2
    val records0 = recordsToStrings(dataGen.generateInserts("000", numRecords)).toList
    val df0 = spark.read.json(spark.sparkContext.parallelize(records0, 2))
    df0.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshotDF0 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertEquals(numRecords, snapshotDF0.count())

    val df1 = snapshotDF0.limit(numRecordsToDelete)
    val dropDf = df1.drop(df1.columns.filter(_.startsWith("_hoodie_")): _*)
    val df2 = convertColumnsToNullable(
      dropDf.withColumn("_hoodie_is_deleted", lit(true).cast(BooleanType)),
      "_hoodie_is_deleted"
    )
    df2.write.format("org.apache.hudi")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .load(basePath)
    assertEquals(numRecords - numRecordsToDelete, snapshotDF2.count())
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testWriteSmallPrecisionDecimalTable(recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType)
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
      .withColumn("shortDecimal", lit(new java.math.BigDecimal(s"2090.0000"))) // create decimalType(8, 4)
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // update the value of shortDecimal
    val inputDF2 = inputDF1.withColumn("shortDecimal", lit(new java.math.BigDecimal(s"3090.0000")))
    inputDF2.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val readResult = spark.read.format("hudi").options(readOpts).load(basePath)
    assert(readResult.count() == 5)
    // compare the test result
    assertEquals(inputDF2.sort("_row_key").select("shortDecimal").collect().map(_.getDecimal(0).toPlainString).mkString(","),
      readResult.sort("_row_key").select("shortDecimal").collect().map(_.getDecimal(0).toPlainString).mkString(","))
  }

  @ParameterizedTest
  @CsvSource(Array(
    "true, true, AVRO", "true, false, AVRO", "true, true, SPARK", "true, false, SPARK",
    "false, true, AVRO", "false, false, AVRO", "false, true, SPARK", "false, false, SPARK"
  ))
  def testPartitionColumnsProperHandling(enableFileIndex: Boolean,
                                         useGlobbing: Boolean,
                                         recordType: HoodieRecordType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, enableFileIndex = enableFileIndex)

    val _spark = spark
    import _spark.implicits._

    val df = Seq((1, "z3", 30, "v1", "2018-09-23"), (2, "z3", 35, "v1", "2018-09-24"))
      .toDF("id", "name", "age", "ts", "data_date")

    df.write.format("hudi")
      .options(writeOpts)
      .option("hoodie.insert.shuffle.parallelism", "4")
      .option("hoodie.upsert.shuffle.parallelism", "4")
      .option("hoodie.bulkinsert.shuffle.parallelism", "2")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "data_date")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, "org.apache.hudi.keygen.TimestampBasedKeyGenerator")
      .option(TIMESTAMP_TYPE_FIELD.key, "DATE_STRING")
      .option(TIMESTAMP_INPUT_DATE_FORMAT.key, "yyyy-MM-dd")
      .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy/MM/dd")
      .option(TIMESTAMP_TIMEZONE_FORMAT.key, "GMT+8:00")
      .mode(org.apache.spark.sql.SaveMode.Append)
      .save(basePath)

    // NOTE: We're testing here that both paths are appropriately handling
    //       partition values, regardless of whether we're reading the table
    //       t/h a globbed path or not
    val pathForReader = getPathForReader(basePath, useGlobbing || !enableFileIndex, 3)

    // Case #1: Partition columns are read from the data file
    val firstDF = spark.read.format("hudi").options(readOpts).load(pathForReader)

    assert(firstDF.count() == 2)

    // data_date is the partition field. Persist to the parquet file using the origin values, and read it.
    // TODO(HUDI-3204) we have to revert this to pre-existing behavior from 0.10
    val expectedValues = if (useGlobbing || !enableFileIndex) {
      Seq("2018-09-23", "2018-09-24")
    } else {
      Seq("2018/09/23", "2018/09/24")
    }

    assertEquals(expectedValues, firstDF.select("data_date").map(_.get(0).toString).collect().sorted.toSeq)
    assertEquals(
      Seq("2018/09/23", "2018/09/24"),
      firstDF.select("_hoodie_partition_path").map(_.get(0).toString).collect().sorted.toSeq
    )

    // Case #2: Partition columns are extracted from the partition path
    //
    // NOTE: This case is only relevant when globbing is NOT used, since when globbing is used Spark
    //       won't be able to infer partitioning properly
    if (!useGlobbing && enableFileIndex) {
      val secondDF = spark.read.format("hudi")
        .options(readOpts)
        .option(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key, "true")
        .load(pathForReader)

      assert(secondDF.count() == 2)

      // data_date is the partition field. Persist to the parquet file using the origin values, and read it.
      assertEquals(
        Seq("2018/09/23", "2018/09/24"),
        secondDF.select("data_date").map(_.get(0).toString).collect().sorted.toSeq
      )
      assertEquals(
        Seq("2018/09/23", "2018/09/24"),
        secondDF.select("_hoodie_partition_path").map(_.get(0).toString).collect().sorted.toSeq
      )
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testSaveAsTableInDifferentModes(recordType: HoodieRecordType): Unit = {
    val options = scala.collection.mutable.Map.empty ++ commonOpts ++ Map("path" -> basePath)
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, options.toMap)

    // first use the Overwrite mode
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .saveAsTable("hoodie_test")

    // init metaClient
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf)
      .build()
    assertEquals(spark.read.format("hudi").options(readOpts).load(basePath).count(), 5)

    // use the Append mode
    val records2 = recordsToStrings(dataGen.generateInserts("002", 6)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(writeOpts)
      .mode(SaveMode.Append)
      .saveAsTable("hoodie_test")
    assertEquals(spark.read.format("hudi").options(readOpts).load(basePath).count(), 11)

    // use the Ignore mode
    val records3 = recordsToStrings(dataGen.generateInserts("003", 7)).toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(writeOpts)
      .mode(SaveMode.Ignore)
      .saveAsTable("hoodie_test")
    // nothing to do for the ignore mode
    assertEquals(spark.read.format("hudi").options(readOpts).load(basePath).count(), 11)

    // use the ErrorIfExists mode
    val records4 = recordsToStrings(dataGen.generateInserts("004", 8)).toList
    val inputDF4 = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    try {
      inputDF4.write.format("org.apache.hudi")
        .partitionBy("partition")
        .options(writeOpts)
        .mode(SaveMode.ErrorIfExists)
        .saveAsTable("hoodie_test")
    } catch {
      case e: Throwable => // do nothing
    }

    // use the Overwrite mode
    val records5 = recordsToStrings(dataGen.generateInserts("005", 9)).toList
    val inputDF5 = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .partitionBy("partition")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .saveAsTable("hoodie_test")
    assertEquals(spark.read.format("hudi").options(readOpts).load(basePath).count(), 9)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testMetricsReporterViaDataSource(recordType: HoodieRecordType): Unit = {
    val (writeOpts, _) = getWriterReaderOpts(recordType, getQuickstartWriteConfigs.asScala.toMap)

    val dataGenerator = new QuickstartUtils.DataGenerator()
    val records = convertToStringList(dataGenerator.generateInserts(10))
    val recordsRDD = spark.sparkContext.parallelize(records, 2)
    val inputDF = spark.read.json(sparkSession.createDataset(recordsRDD)(Encoders.STRING))
    inputDF.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "uuid")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "partitionpath")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.TBL_NAME.key, "hoodie_test")
      .option(HoodieMetricsConfig.TURN_METRICS_ON.key(), "true")
      .option(HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key(), MetricsReporterType.INMEMORY.name)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    assertEquals(false, Metrics.isInitialized(basePath), "Metrics should be shutdown")
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testMapArrayTypeSchemaEvolution(recordType: HoodieRecordType): Unit = {
    assertDoesNotThrow(
      new Executable {
        override def execute(): Unit = {
          val (writeOpts, _) = getWriterReaderOpts(recordType, getQuickstartWriteConfigs.asScala.toMap)

          val schema1 = StructType(
            StructField("_row_key", StringType, nullable = false) ::
              StructField("name", MapType(StringType,
                ArrayType(StringType, containsNull = false)), nullable = true) ::
              StructField("timestamp", LongType, nullable = true) ::
              StructField("partition", LongType, nullable = true) :: Nil)
          val records = List(Row("1", null, 1L, 1L))
          val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(records, 2), schema1)
          inputDF.write.format("org.apache.hudi")
            .options(commonOpts ++ writeOpts)
            .mode(SaveMode.Overwrite)
            .save(basePath)

          val schema2 = StructType(StructField("_row_key", StringType, nullable = false) ::
            StructField("name", MapType(StringType, ArrayType(StringType,
              containsNull = true)), nullable = true) ::
            StructField("timestamp", LongType, nullable = true) ::
            StructField("partition", LongType, nullable = true) :: Nil)
          val records2 = List(Row("1", null, 1L, 1L))
          val inputDF2 = spark.createDataFrame(spark.sparkContext.parallelize(records2, 2), schema2)
          inputDF2.write.format("org.apache.hudi")
            .options(commonOpts ++ writeOpts)
            .mode(SaveMode.Append)
            .save(basePath)
        }
      })
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testMapArrayTypeSchemaEvolutionDuringMerge(recordType: HoodieRecordType): Unit = {
    assertDoesNotThrow(
      new Executable {
        override def execute(): Unit = {
          val (writeOpts, _) = getWriterReaderOpts(recordType, getQuickstartWriteConfigs.asScala.toMap)

          val schema1 = StructType(
            StructField("_row_key", StringType, nullable = false) ::
              StructField("map_col", MapType(StringType, StringType, false)) ::
              StructField("array_col", ArrayType(LongType, containsNull = false)) ::
              StructField("timestamp", LongType, nullable = true) ::
              StructField("partition", LongType, nullable = true) :: Nil)
          val records = List(Row("1", Map("foo"-> "bar"), Array(1L), 1L, 1L))
          val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(records, 2), schema1)
          inputDF.write.format("org.apache.hudi")
            .options(commonOpts ++ writeOpts)
            .mode(SaveMode.Overwrite)
            .save(basePath)

          val schema2 = StructType(StructField("_row_key", StringType, nullable = false) ::
            StructField("map_col", MapType(StringType, StringType, true)) ::
            StructField("array_col", ArrayType(LongType, containsNull = true)) ::
            StructField("timestamp", LongType, nullable = true) ::
            StructField("partition", LongType, nullable = true) :: Nil)
          val records2 = List(Row("2", Map.empty, Array.empty, 1L, 1L))
          val inputDF2 = spark.createDataFrame(spark.sparkContext.parallelize(records2, 2), schema2)
          inputDF2.write.format("org.apache.hudi")
            .options(commonOpts ++ writeOpts)
            .mode(SaveMode.Append)
            .save(basePath)
        }
      })
  }

  def getWriterReaderOpts(recordType: HoodieRecordType,
                          opt: Map[String, String] = commonOpts,
                          enableFileIndex: Boolean = DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX.defaultValue()):
  (Map[String, String], Map[String, String]) = {
    val fileIndexOpt: Map[String, String] =
      Map(DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX.key -> enableFileIndex.toString)

    recordType match {
      case HoodieRecordType.SPARK => (opt ++ sparkOpts, sparkOpts ++ fileIndexOpt)
      case _ => (opt, fileIndexOpt)
    }
  }

  def getWriterReaderOptsLessPartitionPath(recordType: HoodieRecordType,
                                           opt: Map[String, String] = commonOpts,
                                           enableFileIndex: Boolean = DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX.defaultValue()):
  (Map[String, String], Map[String, String]) = {
    val (writeOpts, readOpts) = getWriterReaderOpts(recordType, opt, enableFileIndex)
    (writeOpts.-(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()), readOpts)
  }

  def getPathForReader(basePath: String, useGlobbing: Boolean, partitionPathLevel: Int): String = {
    if (useGlobbing) {
      // When explicitly using globbing or not using HoodieFileIndex, we fall back to the old way
      // of reading Hudi table with globbed path
      basePath + "/*" * (partitionPathLevel + 1)
    } else {
      basePath
    }
  }

  @Test
  def testHiveStyleDelete(): Unit = {
    val columns = Seq("id", "precombine", "partition")
    val data = Seq((1, "1", "2021-01-05"),
      (2, "2", "2021-01-06"),
      (3, "3", "2021-01-05"))
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns: _*)
    var hudiOptions = Map[String, String](
      HoodieWriteConfig.TBL_NAME.key() -> "tbl",
      DataSourceWriteOptions.OPERATION.key() -> "insert",
      DataSourceWriteOptions.TABLE_TYPE.key() -> "COPY_ON_WRITE",
      DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "id",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "partition",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "precombine",
      DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key() -> "true"
    )

    df.write.format("org.apache.hudi").options(hudiOptions).mode(SaveMode.Overwrite).save(basePath)

    hudiOptions = Map[String, String](
      HoodieWriteConfig.TBL_NAME.key() -> "tbl",
      DataSourceWriteOptions.OPERATION.key() -> "delete",
      DataSourceWriteOptions.TABLE_TYPE.key() -> "COPY_ON_WRITE",
      DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "id",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "partition",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "precombine"
    )

    df.filter(df("id") === 1).
      write.format("org.apache.hudi").options(hudiOptions).
      mode(SaveMode.Append).save(basePath)
    val result = spark.read.format("hudi").load(basePath)
    assertEquals(2, result.count())
    assertEquals(0, result.filter(result("id") === 1).count())
  }

  /** Test case to verify MAKE_NEW_COLUMNS_NULLABLE config parameter. */
  @Test
  def testSchemaEvolutionWithNewColumn(): Unit = {
    val df1 = spark.sql("select '1' as event_id, '2' as ts, '3' as version, 'foo' as event_date")
    var hudiOptions = Map[String, String](
      HoodieWriteConfig.TBL_NAME.key() -> "test_hudi_merger",
      KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key() -> "event_id",
      KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key() -> "version",
      DataSourceWriteOptions.OPERATION.key() -> "insert",
      HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key() -> "ts",
      HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key() -> "org.apache.hudi.keygen.ComplexKeyGenerator",
      KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key() -> "true",
      HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key() -> "false",
      HoodieWriteConfig.RECORD_MERGER_IMPLS.key() -> "org.apache.hudi.HoodieSparkRecordMerger"
    )
    df1.write.format("hudi").options(hudiOptions).mode(SaveMode.Append).save(basePath)

    // Try adding a string column. This operation is expected to throw 'schema not compatible' exception since
    // 'MAKE_NEW_COLUMNS_NULLABLE' parameter is 'false' by default.
    val df2 = spark.sql("select '2' as event_id, '2' as ts, '3' as version, 'foo' as event_date, 'bar' as add_col")
    try {
      (df2.write.format("hudi").options(hudiOptions).mode("append").save(basePath))
      fail("Option succeeded, but was expected to fail.")
    } catch {
      case ex: org.apache.hudi.exception.HoodieInsertException => {
        assertTrue(ex.getMessage.equals("Failed insert schema compatibility check"))
      }
      case ex: Exception => {
        fail(ex)
      }
    }

    // Try adding the string column again. This operation is expected to succeed since 'MAKE_NEW_COLUMNS_NULLABLE'
    // parameter has been set to 'true'.
    hudiOptions = hudiOptions + (HoodieCommonConfig.MAKE_NEW_COLUMNS_NULLABLE.key() -> "true")
    try {
      (df2.write.format("hudi").options(hudiOptions).mode("append").save(basePath))
    } catch {
      case ex: Exception => {
        fail(ex)
      }
    }
  }

  def assertLastCommitIsUpsert(): Boolean = {
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(hadoopConf)
      .build()
    val timeline = metaClient.getActiveTimeline.getAllCommitsTimeline
    val latestCommit = timeline.lastInstant()
    assert(latestCommit.isPresent)
    assert(latestCommit.get().isCompleted)
    val metadata = TimelineUtils.getCommitMetadata(latestCommit.get(), timeline)
    metadata.getOperationType.equals(WriteOperationType.UPSERT)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieRecordType], names = Array("AVRO", "SPARK"))
  def testInsertOverwriteCluster(recordType: HoodieRecordType): Unit = {
    val (writeOpts, _) = getWriterReaderOpts(recordType)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    val optsWithCluster = Map(
      INLINE_CLUSTERING_ENABLE.key() -> "true",
      "hoodie.clustering.inline.max.commits" -> "2",
      "hoodie.clustering.plan.strategy.sort.columns" -> "_row_key",
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    ) ++ writeOpts
    inputDF.write.format("hudi")
      .options(optsWithCluster)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    for (i <- 1 until 6) {
      val records = recordsToStrings(dataGen.generateInsertsForPartition("00" + i, 10, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      inputDF.write.format("hudi")
        .options(optsWithCluster)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
    }

    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(hadoopConf)
      .build()
    val timeline = metaClient.getActiveTimeline
    val instants = timeline.getAllCommitsTimeline.filterCompletedInstants.getInstants
    assertEquals(9, instants.size)
    val replaceInstants = instants.filter(i => i.getAction.equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).toList
    assertEquals(8, replaceInstants.size)
    val clusterInstants = replaceInstants.filter(i => {
      TimelineUtils.getCommitMetadata(i, metaClient.getActiveTimeline).getOperationType.equals(WriteOperationType.CLUSTER)
    })
    assertEquals(3, clusterInstants.size)
  }
}

object TestCOWDataSource {
  def convertColumnsToNullable(df: DataFrame, cols: String*): DataFrame = {
    cols.foldLeft(df) { (df, c) =>
      // NOTE: This is the trick to make Spark convert a non-null column "c" into a nullable
      //       one by pretending its value could be null in some execution paths
      df.withColumn(c, when(col(c).isNotNull, col(c)).otherwise(lit(null)))
    }
  }
}
