package com.it.userportrait.sink;

import com.it.userportrait.analy.EntanglementProductEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EntanglementProductAnlaySink implements SinkFunction<EntanglementProductEntity> {

    public void invoke(EntanglementProductEntity value, Context context) throws Exception {
        if(value != null) {
            String groupyField = value.getGroupField();
            String [] groupfileds = groupyField.split("==");///productType==timehour==productTypeid
            String timeinfo = groupfileds[1];
            String productid = groupfileds[2];
            long numbers = value.getNumbers();
           String timeInfoString = DateUtils.getDateString(Long.valueOf(timeinfo),"yyyyMMddhh");
            String tablename = "entanglementProducInfo";
            Map<String, String> dataMap = new HashMap<String, String>();
            dataMap.put("productid",productid);
            dataMap.put("timeinfo",timeinfo);
            dataMap.put("numbers",numbers+"");
            dataMap.put("timeInfoString",timeInfoString);
            Set<String> intFieldSet = new HashSet<String>();
            intFieldSet.add("numbers");
            ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
        }
    }
}
