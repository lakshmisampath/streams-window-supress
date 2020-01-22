package com.lakshmisampath.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class MessageDeserializer implements Deserializer<Message> {

  @Override
  public void close() {}

  @Override
  public void configure(Map configs, boolean isKey) {}

  @Override
  public Message deserialize(String topic, byte[] data) {
    if (data==null){
      return new Message("",0L, null);
    }

    ObjectMapper mapper = new ObjectMapper();
    Message record = null;
    try {
      record = mapper.readValue(data, Message.class);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return record;
  }

}