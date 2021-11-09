package com.it.userportrait.analy;

import com.it.userportrait.map.WoolWarningAnlayMap;
import com.it.userportrait.reduce.WoolWarningAnlayReduce;
import com.it.userportrait.sink.WoolWaringAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-21 15:27
 */
public class WoolWarningAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "user");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("order", new SimpleStringSchema(), properties);
        //        //指定偏移量
//        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

        env.enableCheckpointing(5000);
        DataStream<WoolEntity> map = stream.map(new WoolWarningAnlayMap());
        DataStream<WoolEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.hours(1))
                .reduce(new WoolWarningAnlayReduce());
        reduce.addSink(new WoolWaringAnlaySink());
        try {
            env.execute("WoolWarningAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
