package com.lakshmisampath.kafka.streams;

import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


public class MessageCompare {

  public static void main(String[] args) {

    // Producer and Consumer config
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    properties.setProperty("sasl.mechanism", "PLAIN");
    properties.setProperty("sasl.jaas.config", String.format(
      "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "username=\"%s\" password=\"%s\";",
      Config.KAFKA_USER, Config.KAFKA_PASSWORD));

    String datePattern="E, dd MMM yyyy HH:mm:ss z";
    SimpleDateFormat simpleDateFormat =new SimpleDateFormat(datePattern);

    Map<String,Message> inputMap = getMapFromTopic(properties, Config.INPUT_TOPIC_NAME, Config.START_HOUR_TO_COMPARE, Config.END_HOUR_TO_COMPARE);
    Map<String,Message> outputMap = getMapFromTopic(properties, Config.OUTPUT_TOPIC_NAME,Config.START_HOUR_TO_COMPARE, Config.END_HOUR_TO_COMPARE);
    System.out.println("\n\n===========\nInput topic records:"+inputMap.entrySet().size());
    inputMap.entrySet().stream()
      .forEach(e -> System.out.println(String.format("%s,%s (%s)",
        e.getValue().getId(),
        simpleDateFormat.format(e.getValue().getTimestampInMillis()),
        e.getValue().getTimestampInMillis()
        )));
    System.out.println("\n\n===========\nOutput topic records:"+outputMap.entrySet().size());
    outputMap.entrySet().stream()
      .forEach(e -> System.out.println(String.format("%s,%s (%s)",
        e.getValue().getId(),
        simpleDateFormat.format(e.getValue().getTimestampInMillis()),
        e.getValue().getTimestampInMillis()
      )));
  }

  public static Map<String, Message> getMapFromTopic(Properties properties, String topic, int startHour, int endHour){
    Map<String, Message> hourlyMap = new HashMap<>();
    Consumer<String, Message> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(topic));
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
        String hourlyKey = record.value().getId() + ":" + record.value().getTimestampInMillis();

        if (record.value().getTimestampInMillis() >= (Config.INITIAL_TIME + (60 * 60 * 1000 * startHour)) &&
              record.value().getTimestampInMillis() <= (Config.INITIAL_TIME + (60 * 60 * 1000 * endHour))){
          hourlyMap.put(hourlyKey, record.value());
        }
      });
    }
    consumer.commitAsync();
    consumer.close();
    return hourlyMap;
  }
}
