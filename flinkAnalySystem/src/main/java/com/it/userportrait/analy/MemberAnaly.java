package com.it.userportrait.analy;

import com.it.userportrait.map.MemberAnlayMap;
import com.it.userportrait.reduce.MemberAnlayReduce;
import com.it.userportrait.sink.MemberAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description: TODO 用户会员标签
 * @author: Delusion
 * @date: 2021-07-23 8:40
 */
public class MemberAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-11:9092");
        properties.setProperty("group.id", "member");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("userinfo", new SimpleStringSchema(), properties);
        //        //指定偏移量
        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

//        env.enableCheckpointing(5000);
        stream.print();
        DataStream<MemberEntity> map = stream.map(new MemberAnlayMap());
        DataStream<MemberEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.seconds(3))
                .reduce(new MemberAnlayReduce());
        reduce.addSink(new MemberAnlaySink());
        try {
            env.execute("MemberAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

