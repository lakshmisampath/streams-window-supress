package com.lakshmisampath.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;

public class MessageSerde {

  private MessageSerde() {
  }

  public static Serde<Message> getMessageSerde() {
    return Serdes.serdeFrom(new MessageSerializer(), new MessageDeserializer());
  }

  /**
   * Creates Windowed string serde.
   * @return
   */
  public static Serde<Windowed<String>> getWindowedStringSerde() {
    // Windowed Key SerializerDeserializer
    final TimeWindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>(
      new StringSerializer());
    final TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(
      new StringDeserializer());
    return Serdes.serdeFrom(
      windowedSerializer,windowedDeserializer);
  }

}
