package com.it.userportrait.sink;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.WoolEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WoolAnlaySink implements SinkFunction<WoolEntity> {
    @Override
    public void invoke(WoolEntity value, Context context) throws Exception {
        String timeinfo = value.getTimeinfo();
        String timeinfoString = DateUtils.getDateString(Long.valueOf(timeinfo),"yyyyMMdd");
        long nubmers = value.getNumbers();
        String tablename = "wooldayinfo";
        Map<String, String> dataMap = new HashMap<String, String>();
        dataMap.put("timeinfo",timeinfo);
        dataMap.put("numbers",nubmers+"");
        dataMap.put("timeinfoString",timeinfoString);
        Set<String> intFieldSet = new HashSet<String>();
        intFieldSet.add("numbers");
        ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
    }
}
