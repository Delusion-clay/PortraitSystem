package com.it.userportrait.analy;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.bean.Order;
import com.it.userportrait.log.AdvisterOpertor;
import com.it.userportrait.map.AdverTypeMarketingSensitivityAnlayMap;
import com.it.userportrait.map.MarketingSensitivityAnlayMap;
import com.it.userportrait.reduce.AdverTypeMarketingSensitivityAnlayReduce;
import com.it.userportrait.reduce.MarketingSensitivityAnlayReduce;
import com.it.userportrait.sink.AvderTypeMarketingSensitivityAnlaySink;
import com.it.userportrait.sink.MarketingSensitivityAnlaySink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description:  广告敏感度计算
 * @author: Delusion
 * @date: 2021-08-04 14:48
 */
public class MarketingSensitivityAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "portrait");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumerAdviser = new FlinkKafkaConsumer<String>("adviser", new SimpleStringSchema(), properties);
        //        //指定偏移量
//        myConsumer.setStartFromEarliest();

        final DataStream<String> streamAdviser = env
                .addSource(myConsumerAdviser);

        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumerOrder = new FlinkKafkaConsumer<String>("order", new SimpleStringSchema(), properties);
        //        //指定偏移量
        myConsumerOrder.setStartFromEarliest();

        final DataStream<String> streamOrder = env
                .addSource(myConsumerOrder);

        env.enableCheckpointing(5000);

        DataStream<JSONObject> streamjoin = streamAdviser.coGroup(streamOrder).where(new KeySelector<String, String>() {
            @Override
            public  String getKey(String s) throws Exception {
                AdvisterOpertor advisterOpertor = JSONObject.parseObject(s, AdvisterOpertor.class);
                long adviId = advisterOpertor.getAdviId();
                return advisterOpertor.getUserid()+"=="+adviId;
            }
        }).equalTo(new KeySelector<String,  String>() {
            @Override
            public  String getKey(String s) throws Exception {
                Order order = JSONObject.parseObject(s, Order.class);
                long adviId = order.getAdvisterId();
                return order.getUserid() + "==" + adviId;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(1))).apply(new LeftJoin());

//        }).window(TumblingEventTimeWindows.of(Time.hours(1))).apply(new JoinFunction<String, String, JSONObject>() {
//            @Override
//            public JSONObject join(String s, String s2) throws Exception {
//                JSONObject jsonObject = new JSONObject();
//                jsonObject.put("adviser",s);
//                jsonObject.put("orderinfo",s2);
//                return jsonObject;
//            }
//        });


        DataStream<MarketingSensitivityEntity> map = streamjoin.map(new MarketingSensitivityAnlayMap());
        DataStream<MarketingSensitivityEntity> reduce=  map.keyBy("groupField").timeWindowAll(Time.seconds(1))
                .reduce(new MarketingSensitivityAnlayReduce());
        DataStream<MarketingSensitivityEntity> adtypeMap = reduce.map(new AdverTypeMarketingSensitivityAnlayMap());
        DataStream<MarketingSensitivityEntity> adtypereduce = adtypeMap.keyBy("groupField").timeWindowAll(Time.seconds(1)).reduce(new AdverTypeMarketingSensitivityAnlayReduce());
        adtypereduce.addSink(new AvderTypeMarketingSensitivityAnlaySink());
        reduce.addSink(new MarketingSensitivityAnlaySink());
        try {
            env.execute("MarketingSensitivityAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
