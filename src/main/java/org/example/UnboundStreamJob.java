package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Hello world!
 */

public class UnboundStreamJob {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (lines, out) -> {
            Arrays.stream(lines.split(" ")).forEach(s -> out.collect(Tuple2.of(s, 1)));
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).sum(1);
        sum.print("test");
        env.execute();
    }
}
