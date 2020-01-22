package com.lakshmisampath.kafka.streams;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuppressMain {


  private static final Logger logger = LoggerFactory.getLogger(SuppressMain.class);

  public static void main(String[] args) {

    // Steams config
    Properties streamsConfig = new Properties();
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.STREAMS_APP_ID);
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP);
    streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
    streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    streamsConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    streamsConfig.put("sasl.mechanism", "PLAIN");
    streamsConfig.put("sasl.jaas.config", String.format(
      "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "username=\"%s\" password=\"%s\";",
      Config.KAFKA_USER, Config.KAFKA_PASSWORD));


    // Streams application
    final MessageTopology msgTopologyBuilder = new MessageTopology();
    Topology msgTopology = msgTopologyBuilder.createTopology(
      Config.INPUT_TOPIC_NAME,Config.OUTPUT_TOPIC_NAME, Config.WINDOWING_TIME_IN_SEC,
      Config.GRACE_PERIOD_IN_HRS, Config.STREAM_STORE_NAME, Config.STREAM_STORE_RETENTION_TIME_IN_HRS);
    final KafkaStreams streams = new KafkaStreams(msgTopology, streamsConfig);
    streams.cleanUp();
    streams.start();
    logger.info(streams.toString());
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

}