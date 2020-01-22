package com.lakshmisampath.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MessageSerializer implements Serializer<Message> {
  @Override
  public void configure(Map configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, Message record) {
    byte[] retVal = null;

    ObjectMapper objectMapper = new ObjectMapper();
    try {
      retVal = objectMapper.writeValueAsString(record).getBytes();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return retVal;
  }

  @Override
  public void close() {
  }
}