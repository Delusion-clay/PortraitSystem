package com.it.userportrait.sink;
import com.it.userportrait.analy.CarrierEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CarrierAnlaySink implements SinkFunction<CarrierEntity> {

    public void invoke(CarrierEntity value, Context context) throws Exception {
        if(value != null) {
            String groupyField = value.getGroupField();
            String [] groupfileds = groupyField.split("==");
            String timeinfo = groupfileds[1];
            String timeinfoString = DateUtils.getDateString(Long.valueOf(timeinfo),"yyyyMMddHHmm");
            String carrierString = groupfileds[2];
            long numbers = value.getNumbers();
            String tablename = "carrierinfo";
            Map<String, String> dataMap = new HashMap<String, String>();
            dataMap.put("carrierString",carrierString);
            dataMap.put("timeinfodate",timeinfo);
            dataMap.put("numbers",numbers+"");
            dataMap.put("timeinfoString",timeinfoString+"");
            Set<String> intFieldSet = new HashSet<String>();
            intFieldSet.add("numbers");
            ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
        }
    }
}

