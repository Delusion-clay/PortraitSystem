package com.it.userportrait.sink;

import com.it.userportrait.analy.MarketingSensitivityEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MarketingSensitivityAnlaySink implements SinkFunction<MarketingSensitivityEntity> {


    public void invoke(MarketingSensitivityEntity value, Context context) throws Exception {
        if(value != null) {
            String groupyField = value.getGroupField();
            String [] groupfileds = groupyField.split("==");// "MarketingSensitivity=="+timeinfo+"=="+userid+"=="+advisterId;
            String timeinfo = groupfileds[1];
            String userId = groupfileds[2];
            String advisterId = groupfileds[3];
            long advernums = value.getAdvernums();
            long ordernums = value.getOrdernums();
            //非常敏感2或者点击5+ + 1 一般5+ 或者1+ 1  不敏感2
            String sensitivityFlag = "";
            if(advernums<=2&&ordernums==0){
                sensitivityFlag = "不敏感";
                //不敏感
            }else if((advernums>5&&advernums==0)||(advernums>1&&advernums==1)){
                sensitivityFlag = "一般";
                //一般
            }else if((advernums>1&&advernums>1)||(advernums>5&&advernums==1)){
                sensitivityFlag = "非常敏感";
                //非常敏感
            }
            String tablename = "useradSensitivityinfo";
            Map<String, String> dataMap = new HashMap<String, String>();
            dataMap.put("userId",userId);
            dataMap.put("advisterId",advisterId);
            dataMap.put("timeinfo",timeinfo);
            dataMap.put("advernums",advernums+"");
            dataMap.put("ordernums",ordernums+"");
            dataMap.put("sensitivityFlag",sensitivityFlag);
            String timeinfoString = DateUtils.getDateString(Long.valueOf(timeinfo),"yyyyMMddhh");
            dataMap.put("timeinfoString",timeinfoString+"");
            Set<String> intFieldSet = new HashSet<String>();
            intFieldSet.add("advernums");
            intFieldSet.add("ordernums");
            ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
        }
    }
}
