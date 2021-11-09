package com.it.userportrait.analy;

import com.it.userportrait.map.CarrierAnlayMap;
import com.it.userportrait.reduce.CarrierAnlayReduce;
import com.it.userportrait.sink.CarrierAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description: TODO 用户手机号运营商标签
 * @author: Delusion
 * @date: 2021-07-22 9:00
 */
public class CarrierAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "carrier");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("userinfo", new SimpleStringSchema(), properties);
        //        //指定偏移量
        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

        env.enableCheckpointing(5000);
        DataStream<CarrierEntity> map = stream.map(new CarrierAnlayMap());
        DataStream<CarrierEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.seconds(3))
                .reduce(new CarrierAnlayReduce());
        reduce.addSink(new CarrierAnlaySink());
        try {
            env.execute("CarrierAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
