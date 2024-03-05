package com.twalthr.flink.examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Instant;
import java.time.LocalDate;

public class FillKafkaWithWeblogs {

  public static void main(String[] args) {
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//    DataStream<String> transactionStream = env.fromElements(ExampleData.WEBLOGS);
//
//    tableEnv
//            .fromDataStream(transactionStream)
//            .executeInsert(KafkaDescriptors.WEBLOGS_DESCRIPTOR);
  }
}
