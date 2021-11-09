package com.it.userportrait.sink;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.ConsumeEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.ClickUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConsumerUserAnlaySink implements SinkFunction<ConsumeEntity> {

    @Override
    public void invoke(ConsumeEntity value, Context context) throws Exception {
        long nubmers = value.getNumbers();
        long timeinfo = value.getTimeinfo();
        String consumeFlag = value.getConsumeFlag();
        String timeinfoString = DateUtils.getDateString(timeinfo,"yyyyMM");
        String tablename = "consumerFenbuinfo";
        Map<String, String> dataMap = new HashMap<String, String>();
        dataMap.put("nubmers", nubmers+"");
        dataMap.put("timeinfo",timeinfo+"");
        dataMap.put("timeinfoString",timeinfoString);
        dataMap.put("consumeFlag",consumeFlag+"");
        ClickUtils.saveData(tablename,dataMap);
        Set<String> intFieldSet = new HashSet<String>();
        intFieldSet.add("numbers");
        ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
    }
}
