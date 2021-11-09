package com.it.userportrait.analy;

import com.it.userportrait.map.ProductTypeMap;
import com.it.userportrait.reduce.ProductTypeAnlayReduce;
import com.it.userportrait.sink.ProductTypeAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description: TODO 用户小时级别的商品偏好
 * @author: Delusion
 * @date: 2021-07-27 0:13
 */
public class ProductTypeAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "ProductType");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("scan", new SimpleStringSchema(), properties);
        //        //指定偏移量
//        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

        env.enableCheckpointing(5000);
        DataStream<ProducttypeEntity> map = stream.map(new ProductTypeMap());
        DataStream<ProducttypeEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.hours(1))
                .reduce(new ProductTypeAnlayReduce());
        reduce.addSink(new ProductTypeAnlaySink());
        try {
            env.execute("ProductTypeAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
