package com.feng.flinkstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author fengyadong
 * @date 2022/5/31 11:51
 * @Description
 */
public class KafkaSourceWordCount {

    public static void main(String[] args) throws Exception {
        // kafka 的属性配置
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "group1");
        // source为来自Kafka的数据，这里我们实例化一个消费者，topic为 hotitems
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("hotitems", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        // 设置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.addSource(consumer);

        SingleOutputStreamOperator<Tuple2<String, Integer>> countStream = inputStream
                .map(String::trim)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    for (String word : s.split(" ")) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) (tuple2) -> String.valueOf(tuple2.f0))
                .sum(1);

        // 输出
        countStream.print();
        // docker 需要配置host
        env.execute("Flink Streaming Java Table API Skeleton");
    }

}
