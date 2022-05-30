package com.feng.flinkstart;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;


/**
 * @author fengyadong
 * @Date: 2022/5/26 20:40
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> inputSet = env.readTextFile("/Users/fengyadong/Desktop/hamlet");

        DataSet<Tuple2<String, Integer>> countSet = inputSet.filter(e -> !StringUtils.isBlank(e))
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] words = s.split(" ");
                    for (String word : words) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.INT))
                .groupBy(0)
                .sum(1);
        countSet.print();
    }

}
