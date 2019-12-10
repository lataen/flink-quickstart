package com.jd.lc.wc;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class MyCoFlatMap {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);


        DataStream<String> text1 = env.socketTextStream("localhost", 9000);
        DataStream<String> text2 = env.socketTextStream("localhost", 8888);

        DataStream<Integer> lenStream = text2.map(Integer::valueOf);
        ConnectedStreams<String, Integer> connectedStreams = text1.connect(lenStream);
        DataStream<String> outStream = connectedStreams.flatMap(new StrategyMapFunction());
        outStream.print();

        env.execute("Socket Window WordCount");
    }

    public static class StrategyMapFunction implements CoFlatMapFunction<String, Integer, String> {
        private int len;

        StrategyMapFunction() {
            this.len = 0;
        }

        @Override
        public void flatMap1(String s, Collector<String> collector) throws Exception {
            if (s.length() > this.len) {
                collector.collect(s);
            }
        }

        @Override
        public void flatMap2(Integer integer, Collector<String> collector) throws Exception {
            this.len = integer;
        }
    }
}
