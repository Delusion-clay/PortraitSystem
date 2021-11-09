package com.it.userportrait.analy;

import com.it.userportrait.map.YearsAnlayMap;
import com.it.userportrait.reduce.YearsAnlayReduce;
import com.it.userportrait.sink.YearsAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-07-20 11:10
 */
public class YearsAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "portrait_year");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("userinfo", new SimpleStringSchema(), properties);
        //        //指定偏移量
        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

//        env.enableCheckpointing(5000);
        stream.print();
        DataStream<YearsEntity> map = stream.map(new YearsAnlayMap());
        map.print("-------");
        DataStream<YearsEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.seconds(3))
                .reduce(new YearsAnlayReduce());
        reduce.print("===========");
        reduce.addSink(new YearsAnlaySink());
        try {
            env.execute("YearsAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
