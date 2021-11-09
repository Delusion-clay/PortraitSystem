package com.it.userportrait.sink;

import com.it.userportrait.analy.ProductZhuEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ProductZhuAnlaySink implements SinkFunction<ProductZhuEntity> {

    public void invoke(ProductZhuEntity value, Context context) throws Exception {
        if(value != null) {
            String groupyField = value.getGroupField();
            String [] groupfileds = groupyField.split("==");
            long timeinfo = value.getTimeinfo();
            long numbers = value.getNumbers();
            String productzhuFlag = value.getProductZhuFlag();
            String timeinfoString = DateUtils.getDateString(Long.valueOf(timeinfo),"yyyyMMdd");
            String tablename = "productZhuinfo";
            Map<String, String> dataMap = new HashMap<String, String>();
            dataMap.put("productzhuFlag",productzhuFlag);
            dataMap.put("timeinfo",timeinfo+"");
            dataMap.put("numbers",numbers+"");
            dataMap.put("timeinfoString",timeinfoString+"");
            Set<String> intFieldSet = new HashSet<String>();
            intFieldSet.add("numbers");
            ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
        }
    }
}
