package com.it.userportrait.sink;

import com.it.userportrait.analy.MarketingSensitivityEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AvderTypeMarketingSensitivityAnlaySink implements SinkFunction<MarketingSensitivityEntity> {


    public void invoke(MarketingSensitivityEntity value, Context context) throws Exception {
        if(value != null) {
            String avTypeName = value.getAvTypeName();
            long timeinfo = value.getTimeinfo();
            String sensitivityFlag = value.getSensitivityFlag();
            long numbers = value.getNumbers();
            String timeinfoString = DateUtils.getDateString(timeinfo,"yyyyMMddhh");
            String tablename = "adtypeSensitivityinfo";
            Map<String, String> dataMap = new HashMap<String, String>();
            dataMap.put("avTypeName",avTypeName);
            dataMap.put("sensitivityFlag",sensitivityFlag);
            dataMap.put("timeinfo",timeinfo+"");
            dataMap.put("timeinfoString",timeinfoString);
            dataMap.put("numbers",numbers+"");
            Set<String> intFieldSet = new HashSet<String>();
            intFieldSet.add("numbers");
            ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
        }
    }
}
