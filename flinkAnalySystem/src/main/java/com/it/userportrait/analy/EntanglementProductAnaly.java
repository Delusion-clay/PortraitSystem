package com.it.userportrait.analy;

import com.it.userportrait.map.EntanglementProductMap;
import com.it.userportrait.reduce.EntanglementProductAnlayReduce;
import com.it.userportrait.sink.EntanglementProductAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description: TODO 用户纠结商品
 * @author: Delusion
 * @date: 2021-07-27 20:31
 */
public class EntanglementProductAnaly {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "Entanglement");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("cart", new SimpleStringSchema(), properties);
        //        //指定偏移量
//        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

        env.enableCheckpointing(5000);
        DataStream<EntanglementProductEntity> map = stream.map(new EntanglementProductMap());
        DataStream<EntanglementProductEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.hours(1))
                .reduce(new EntanglementProductAnlayReduce());
        reduce.addSink(new EntanglementProductAnlaySink());
        try {
            env.execute("EntanglementProductAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
