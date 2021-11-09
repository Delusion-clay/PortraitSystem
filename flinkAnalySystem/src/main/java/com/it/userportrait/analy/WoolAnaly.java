package com.it.userportrait.analy;

import com.it.userportrait.map.WoolAnlayMap;
import com.it.userportrait.map.WoolUserSaveAnlayMap;
import com.it.userportrait.reduce.WoolAnlayReduce;
import com.it.userportrait.reduce.WoolGaiKuangAnlayReduce;
import com.it.userportrait.sink.WoolAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-10 15:25
 */
public class WoolAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "wool");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("order", new SimpleStringSchema(), properties);
        //        //指定偏移量
//        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

        env.enableCheckpointing(5000);
        DataStream<WoolEntity> map = stream.map(new WoolAnlayMap());
        DataStream<WoolEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.hours(1))
                .reduce(new WoolAnlayReduce());
        DataStream<WoolEntity> userSaveMap  =  reduce.map(new WoolUserSaveAnlayMap());
        DataStream<WoolEntity> gaikuangReduce = userSaveMap.keyBy("groupField").timeWindowAll(Time.hours(1))
                .reduce(new WoolGaiKuangAnlayReduce());
        gaikuangReduce.addSink(new WoolAnlaySink());
        try {
            env.execute("WoolAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
