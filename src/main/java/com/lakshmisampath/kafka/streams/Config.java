package com.lakshmisampath.kafka.streams;

public class Config {

  // Common
  public static final String BOOTSTRAP = "127.0.0.1:9092";
  public static final String KAFKA_USER = "api_key";
  public static final String KAFKA_PASSWORD = "secret";
  public static final String INPUT_TOPIC_NAME = "input";
  public static final String OUTPUT_TOPIC_NAME = "output";

  // Streams application
  public static final String STREAMS_APP_ID = "suppress-window-app";
  public static final int WINDOWING_TIME_IN_SEC = 1;
  public static final int GRACE_PERIOD_IN_HRS = 2;
  public static final String STREAM_STORE_NAME = "suppress-window-store";
  public static final int STREAM_STORE_RETENTION_TIME_IN_HRS = 3;

  // Producer
  public static final int UNIQUE_MSG_PER_HOUR = 2;
  public static final int HOURS_TO_SEND = 48;
  public static final long INITIAL_TIME = 1577869200000L; // Jan 1st 2020 1:00 AM PST

  // Compare
  public static final int START_HOUR_TO_COMPARE = 0;
  public static final int END_HOUR_TO_COMPARE = 36;
}
