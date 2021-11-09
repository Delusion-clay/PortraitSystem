package com.it.userportrait.analy;

import com.it.userportrait.map.BrandMap;
import com.it.userportrait.map.UserBrandSaveMap;
import com.it.userportrait.reduce.BrandAnlayReduce;
import com.it.userportrait.reduce.BrandByAnlayReduce;
import com.it.userportrait.sink.BrandbyAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-04 17:44
 */
public class BrandAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "aaa");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("scan", new SimpleStringSchema(), properties);
        //        //指定偏移量
//        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

        env.enableCheckpointing(5000);
        DataStream<BrandEntity> map = stream.map(new BrandMap());
        DataStream<BrandEntity> reduce =   map.keyBy("groupField").timeWindowAll(Time.hours(1))
                .reduce(new BrandAnlayReduce());
        DataStream<BrandEntity> usermap = reduce.map(new UserBrandSaveMap());
        DataStream<BrandEntity>  reuduceBy = usermap.keyBy("groupField").timeWindowAll(Time.hours(1))
                .reduce(new BrandByAnlayReduce());
        reuduceBy.addSink(new BrandbyAnlaySink());
        try {
            env.execute("BrandAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
