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

package org.apache.hudi.utilities.testutils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestKafkaTestUtils {
  private static KafkaTestUtils testUtils;

  @BeforeAll
  static void setup() {
    testUtils = new KafkaTestUtils().setup();
  }

  @AfterEach
  void cleanup() {
    testUtils.deleteTopics();
  }

  @AfterAll
  static void teardown() {
    testUtils.teardown();
  }

  @RepeatedTest(10)
  void testCreatedTopicIsImmediatelyReady() throws Exception {
    Properties topicProperties = new Properties();
    topicProperties.setProperty(TopicConfig.RETENTION_MS_CONFIG, "86400000");
    assertTopicReady(topicProperties, "86400000");
  }

  @RepeatedTest(10)
  void testCreatedTopicWithDefaultConfigIsImmediatelyReady() throws Exception {
    assertTopicReady(null, Long.toString(TimeUnit.DAYS.toMillis(7)));
  }

  private void assertTopicReady(Properties topicProperties, String expectedRetentionMs) throws Exception {
    String topic = "topic_readiness_" + UUID.randomUUID();
    testUtils.createTopic(topic, 2, topicProperties);

    Properties clientProperties = new Properties();
    clientProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, testUtils.brokerAddress());
    clientProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    clientProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    // Callers must be able to inspect the topic immediately, without retrying their assertions.
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(clientProperties);
         AdminClient admin = AdminClient.create(clientProperties)) {
      List<PartitionInfo> partitions = consumer.listTopics(Duration.ofSeconds(10)).get(topic);
      assertNotNull(partitions, "Created topic is not visible to a consumer");
      assertEquals(2, partitions.size());
      assertTrue(partitions.stream().allMatch(partition -> partition.leader() != null && partition.leader().id() >= 0));
      ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      Config config = admin.describeConfigs(Collections.singleton(resource)).all().get(10, TimeUnit.SECONDS).get(resource);
      assertEquals(expectedRetentionMs, config.get(TopicConfig.RETENTION_MS_CONFIG).value());
    }
  }
}
