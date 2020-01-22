package com.lakshmisampath.kafka.streams;

import java.util.Arrays;

public class Message {
  private String id;
  private long timestampInMillis;
  private Metadata[] metadata;

  public Message(){
  }

  public Message(String id, long timestampInMillis, Metadata[] metadata) {
    this.id = id;
    this.timestampInMillis = timestampInMillis;
    this.metadata = metadata;
  }

public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public long getTimestampInMillis() {
    return timestampInMillis;
  }

  public void setTimestampInMillis(long timestampInMillis) {
    this.timestampInMillis = timestampInMillis;
  }

  public Metadata[] getMetadata() {
    return metadata;
  }

  public void setMetadata(Metadata[] metadata) {
    this.metadata = metadata;
  }

  @Override
  public String toString() {
    return "Message{" +
      "id='" + id + '\'' +
      ", timestamp=" + timestampInMillis +
      ", metadata=" + Arrays.toString(metadata) +
      '}';
  }
}

class Metadata {
  private String name;
  private String value;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "Metadata{" +
      "name='" + name + '\'' +
      ", value='" + value + '\'' +
      '}';
  }
}


