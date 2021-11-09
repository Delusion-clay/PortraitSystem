package com.it.userportrait.sink;

import com.it.userportrait.analy.YearsEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.ClickUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-07-20 11:21
 */
public class YearsAnlaySink implements SinkFunction<YearsEntity> {

    public void invoke(YearsEntity value, Context context) throws Exception {
        if(value != null) {
            String groupyField = value.getGroupField();
            String [] groupfileds = groupyField.split("==");
            String timeinfo = groupfileds[1];
            String timeinfoString = DateUtils.getDateString(Long.valueOf(timeinfo),"yyyyMMddhhmm");
            String yearlabel = groupfileds[2];
            long numbers = value.getNumbers();
            String tablename = "yearslableinfo";
            Map<String, String> dataMap = new HashMap<String, String>();
            dataMap.put("yearslabel",yearlabel);
            dataMap.put("timeinfodate",timeinfo);
            dataMap.put("numbers",numbers+"");
            dataMap.put("timeinfoString",timeinfoString);
            Set<String> intFieldSet = new HashSet<String>();
            intFieldSet.add("numbers");
            ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
            ClickUtils.saveData(tablename,dataMap);
        }
    }
}

