package com.lakshmisampath.kafka.streams;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MessageProducer {

  private static final Logger logger = LoggerFactory.getLogger(SuppressMain.class);

  public static void main(String[] args) {    Properties properties = new Properties();

    // Producer config
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    properties.setProperty("sasl.mechanism", "PLAIN");
    properties.setProperty("sasl.jaas.config", String.format(
      "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "username=\"%s\" password=\"%s\";",
      Config.KAFKA_USER,Config.KAFKA_PASSWORD));

    Set<String> msgIdSet = new HashSet<>();
    for (int i = 0; i<Config.UNIQUE_MSG_PER_HOUR; i++){
      msgIdSet.add(UUID.randomUUID().toString());
    }

    Producer<String, Message> producer = new KafkaProducer<>(properties);
    for (int i = 0; i<Config.HOURS_TO_SEND; i++){
      Iterator<String> iterator = msgIdSet.iterator();
      long nextHour = Config.INITIAL_TIME + (60 * 60 * 1000 * i);
      while(iterator.hasNext()){
        String msgId = iterator.next();
        Metadata[] metadataArr = new Metadata[1];
        Metadata metadata = new Metadata();
        metadata.setName("sample-key");
        metadata.setValue("sample-value");
        metadataArr[0] = metadata;
        Message msg = new Message(msgId, nextHour, metadataArr);
        producer.send(new ProducerRecord<>(Config.INPUT_TOPIC_NAME, UUID.randomUUID().toString(), msg));
      }
      try {
        Thread.sleep(100);
      }catch(Exception e){
        e.printStackTrace();
      }
    }
    producer.close();
  }
}