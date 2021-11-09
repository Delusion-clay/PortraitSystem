package com.it.userportrait.analy;

import com.it.userportrait.map.ProductZhuAnlayMap;
import com.it.userportrait.map.ProductZhuSaveAnlayMap;
import com.it.userportrait.reduce.ProductZhuAnalyReduce;
import com.it.userportrait.reduce.ProductZhuSaveAnalyReduce;
import com.it.userportrait.sink.ProductZhuAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-09-01 17:05
 */
public class ProductZhuAnaly {
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
        DataStream<ProductZhuEntity> map = stream.map(new ProductZhuAnlayMap());
        DataStream<ProductZhuEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.hours(1l))
                .reduce(new ProductZhuAnalyReduce());
        DataStream<ProductZhuEntity> savemap =   reduce.map(new ProductZhuSaveAnlayMap());
        DataStream<ProductZhuEntity> savereduce = savemap .keyBy("groupField").timeWindowAll(Time.hours(1l))
                .reduce(new ProductZhuSaveAnalyReduce());
        savereduce.addSink(new ProductZhuAnlaySink());
        try {
            env.execute("ProductZhuAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
