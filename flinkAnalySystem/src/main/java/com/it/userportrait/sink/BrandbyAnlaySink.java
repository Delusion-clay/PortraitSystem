package com.it.userportrait.sink;

import com.it.userportrait.analy.BrandEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.ClickUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BrandbyAnlaySink implements SinkFunction<BrandEntity> {

    public void invoke(BrandEntity value, Context context) throws Exception {
        if(value != null) {
            String groupyField = value.getGroupField();
            String timeinfo = value.getTimeinfo();
            String brand = value.getBrand();
            long nubmers = value.getNumbers();
            String timeinfoString = DateUtils.getDateString(Long.valueOf(timeinfo),"yyyyMMddhh");
            String tablename = "brandbyinfo";
            Map<String, String> dataMap = new HashMap<String, String>();
            dataMap.put("brand",brand);
            dataMap.put("timeinfo",timeinfo);
            dataMap.put("numbers",nubmers+"");
            dataMap.put("tiimeinfoString",timeinfoString);
            Set<String> intFieldSet = new HashSet<String>();
            intFieldSet.add("numbers");
            ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
        }
    }
}
