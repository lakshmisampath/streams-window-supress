package com.lakshmisampath.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

public class MessageTopology {

  public Topology createTopology(String inputTopicName, String outputTopicName,
                                 int windowingTimeInSec, int gracePeriodInHrs,
                                 String streamStoreName, int streamStoreRetentionTimeInHrs) {
    StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, Message> messageStream =
      builder.stream(inputTopicName, Consumed.with(
        Serdes.String(), MessageSerde.getMessageSerde())
        .withTimestampExtractor((consumerRecord, previousTimestamp) -> ((Message) consumerRecord.value()).getTimestampInMillis()));

    final KTable<Windowed<String>, Message> suppressedMessage = messageStream
      .selectKey((key, value) -> value.getId())
      .groupByKey(Grouped.with(Serdes.String(), MessageSerde.getMessageSerde()))
      .windowedBy(TimeWindows.of(Duration.ofSeconds(windowingTimeInSec))
        .grace(Duration.ofHours(gracePeriodInHrs)))
      .aggregate(
        () -> null,
        (key, deltaRecord, hourlyRecord) -> mergedMetadata(deltaRecord, hourlyRecord),
        Materialized.<String, Message, WindowStore<Bytes, byte[]>>as(
          streamStoreName)
          .withRetention(Duration.ofHours(streamStoreRetentionTimeInHrs))
          .withKeySerde(Serdes.String()).withValueSerde(MessageSerde.getMessageSerde())
      )
      .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

    suppressedMessage.toStream().to(
      outputTopicName,
      Produced.with(MessageSerde.getWindowedStringSerde(), MessageSerde.getMessageSerde()));

    return builder.build();
  }


  private static Message mergedMetadata(Message deltaRecord, Message hourlyRecord) {

    //first time initialization
    if (hourlyRecord==null || (hourlyRecord !=null && hourlyRecord.getId()=="")){
      return deltaRecord;
    }

    // merge metadata
    int deltaLineItemLength = 0;
    int hourlyLineItemLength = 0;
    if (deltaRecord!=null && deltaRecord.getMetadata()!=null) {
      deltaLineItemLength = deltaRecord.getMetadata().length;
    }
    if (hourlyRecord!=null && hourlyRecord.getMetadata()!=null) {
      hourlyLineItemLength = hourlyRecord.getMetadata().length;
    }
    Metadata[] metadataArr = new Metadata[deltaLineItemLength + hourlyLineItemLength];


    if (hourlyRecord!=null && hourlyRecord.getMetadata()!=null) {
      System.arraycopy(hourlyRecord.getMetadata(), 0, metadataArr, 0, hourlyLineItemLength);
    }
    if (deltaRecord!=null && deltaRecord.getMetadata()!=null) {
      System.arraycopy(deltaRecord.getMetadata(), 0, metadataArr, hourlyLineItemLength, deltaLineItemLength);
    }

    hourlyRecord.setMetadata(metadataArr);
    return hourlyRecord;
  }
}
