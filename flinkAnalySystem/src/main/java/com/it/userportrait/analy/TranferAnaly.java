package com.it.userportrait.analy;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.bean.UserInfo;
import com.it.userportrait.util.EntitiyTranfer;
import com.it.userportrait.util.HbaseUtils2;
import com.it.userportrait.util.KafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.*;
/**
 * @Description //TODO 接收canaltokafka的数据，进行加工与save to hbase以及kafka topic
 * @Date 15:10 2021/7/20
 * @Param
 * @return
 **/
public class TranferAnaly {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-101:9092");
        properties.setProperty("group.id", "portrait");
        //构建FlinkKafkaConsumer
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties);
        //        //指定偏移量
//        myConsumer.setStartFromEarliest();

        final DataStream<String> stream = env
                .addSource(myConsumer);

        env.enableCheckpointing(5000);
        stream.print();
        /**
         * {"data":[{"id":"2","account":"2","password":"2","sex":"2","age":null,"phone":null,"status":null,"weixinaccount":null,"zhifubaoaccount":null,"email":null,"createTime":null,"updateTime":null}],"database":"huaxiang","es":1603005378000,"id":2,"isDdl":false,"mysqlType":{"id":"int(20)","account":"int(20)","password":"varchar(50)","sex":"int(20)","age":"int(20)","phone":"varchar(500)","status":"int(2)","weixinaccount":"varchar(500)","zhifubaoaccount":"varchar(500)","email":"varchar(500)","createTime":"datetime","updateTime":"datetime"},"old":null,"pkNames":["id"],"sql":"","sqlType":{"id":4,"account":4,"password":12,"sex":4,"age":4,"phone":12,"status":4,"weixinaccount":12,"zhifubaoaccount":12,"email":12,"createTime":93,"updateTime":93},"table":"userinfo","ts":1603005377867,"type":"INSERT"}
         */
        DataStream<String> map = stream.map(new MapFunction<String,String>() {
            public String map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String type = jsonObject.getString("type");
                String table = jsonObject.getString("table");
                String database = jsonObject.getString("database");
                String data = jsonObject.getString("data");
                JSONArray jsonArray = JSONObject.parseArray(data);
                List<Map<String,String>> listdata = new ArrayList<Map<String, String>>();
                for(int i=0;i<jsonArray.size();i++){
                    JSONObject jsonObjectinner = jsonArray.getJSONObject(i);
                    String tablename = table;
                    String rowkey = jsonObjectinner.getString("id");
                    String famliyname = "info";
                    Map<String,String> datamap = JSONObject.parseObject(JSONObject.toJSONString(jsonObjectinner), Map.class);
                    datamap.put("database",database);
                    datamap.put("typebefore",HbaseUtils2.getdata(tablename,rowkey,famliyname,"typecurent"));
                    datamap.put("typecurent",type);
                    datamap.put("tablename",table);
                    EntitiyTranfer.transferAndInsertByDataMap(tablename,rowkey,datamap);
                    listdata.add(datamap);
                }
                String result = JSONObject.toJSONString(listdata);
                return result;
            }
        });
        map.addSink(new SinkFunction<String>() {
            public void invoke(String value, Context context) throws Exception {
                List<Map> data = JSONObject.parseArray(value,Map.class);
                for(Map<String,String> map:data){
                    String tablename =  map.get("tablename");
                    KafkaUtils.sendData(tablename,JSONObject.toJSONString(map));
                }
            }
        });
        try {
            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
