package com.lakshmisampath.kafka.streams;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class MessageConsumer {
  public static void main(String[] args) {

    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    properties.setProperty("sasl.mechanism", "PLAIN");
    properties.setProperty("sasl.jaas.config",
      String.format(
        "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"%s\" password=\"%s\";",
        Config.KAFKA_USER, Config.KAFKA_PASSWORD));

    Consumer<String, Message> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(Config.OUTPUT_TOPIC_NAME));

    int noMessageFound = 0;

    while (true) {
      ConsumerRecords<String, Message> consumerRecords = consumer.poll(1000);
      if (consumerRecords.count() == 0) {
        noMessageFound++;
        if (noMessageFound > 1)
          break;
        else
          continue;
      }
      consumerRecords.forEach(record -> {
        System.out.println("Record Key " + record.key());
        System.out.println("Record value " + record.value());
        System.out.println("Record partition " + record.partition());
        System.out.println("Record offset " + record.offset());
      });
      // commits the offset of record to broker.
      consumer.commitAsync();
    }

    consumer.close();
  }
}
