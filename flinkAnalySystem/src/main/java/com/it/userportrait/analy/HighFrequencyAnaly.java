package com.it.userportrait.analy;

import com.it.userportrait.map.HighFrequencyAnlayMap;
import com.it.userportrait.reduce.HighFrequencyAnlayReduce;
import com.it.userportrait.sink.HighFrequencyAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description:  高频下单
 * @author: Delusion
 * @date: 2021-08-10 11:10
 */
public class HighFrequencyAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "aaa");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("order", new SimpleStringSchema(), properties);
        //        //指定偏移量
//        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

        env.enableCheckpointing(5000);
        DataStream<BrushEntity> map = stream.map(new HighFrequencyAnlayMap());
        DataStream<BrushEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.hours(1))
                .reduce(new HighFrequencyAnlayReduce());
        reduce.addSink(new HighFrequencyAnlaySink());
        try {
            env.execute("HighFrequencyAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
