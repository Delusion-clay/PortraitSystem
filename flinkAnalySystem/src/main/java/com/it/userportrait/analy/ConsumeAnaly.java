package com.it.userportrait.analy;

import com.it.userportrait.map.ConsumeAnlayMap;
import com.it.userportrait.map.ConsumeUserAnlayMap;
import com.it.userportrait.reduce.ConsumeAnlayReduce;
import com.it.userportrait.reduce.ConsumerUserAnlayReduce;
import com.it.userportrait.sink.ConsumerUserAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-22 1:05
 */
public class ConsumeAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "youfan");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("orderinfo", new SimpleStringSchema(), properties);
        //        //指定偏移量
//        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

        env.enableCheckpointing(5000);
        DataStream<ConsumeEntity> map = stream.map(new ConsumeAnlayMap());
        DataStream<ConsumeEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.hours(1))
                .reduce(new ConsumeAnlayReduce());
        DataStream<ConsumeEntity> usermap =  reduce.flatMap(new ConsumeUserAnlayMap());
        DataStream<ConsumeEntity> userreduce = usermap.keyBy("groupField").timeWindowAll(Time.hours(1))
                .reduce(new ConsumerUserAnlayReduce());
        userreduce.addSink(new ConsumerUserAnlaySink());
        try {
            env.execute("ConsumeAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
