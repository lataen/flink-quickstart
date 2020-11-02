package com.jd.quickstart.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;

public class WorkCountBySQL {
    public static void main(String[] args) throws Exception {
        //获取运行环境

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //创建一个tableEnvironment
        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(env);


        String words = "hello flink hello world hello es hello stream flink";

        String[] split = words.split("\\W+");

        ArrayList<WordCount> list = new ArrayList<>();

        for (String word : split) {
            WordCount wc = new WordCount(word, 1);
            list.add(wc);
        }

        DataSet<WordCount> input = env.fromCollection(list);
        //DataSet转Table, 指定字段名
        Table table = batchTableEnv.fromDataSet(input, "word,frequency");
        table.printSchema();
        //注册为一个表

        batchTableEnv.registerTable("WordCount", table);

        Table table02 = batchTableEnv.sqlQuery("select word as word, sum(frequency) as frequency from WordCount GROUP BY word");
        //Table转DataSet
        DataSet<WordCount> result = batchTableEnv.toDataSet(table02, WordCount.class);
        result.print();
    }

    public static class WordCount {
        public String word;
        public long frequency;

        public WordCount() {
        }

        public WordCount(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return word + ", " + frequency;
        }
    }
}
