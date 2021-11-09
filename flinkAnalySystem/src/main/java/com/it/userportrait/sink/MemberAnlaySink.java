package com.it.userportrait.sink;

import com.it.userportrait.analy.MemberEntity;
import com.it.userportrait.util.ClickHouseUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MemberAnlaySink implements SinkFunction<MemberEntity> {

    public void invoke(MemberEntity value, Context context) throws Exception {
        if(value != null) {
            String groupyField = value.getGroupField();

            String [] groupfileds = groupyField.split("==");
            String timeinfo = groupfileds[1];
            String memberFlag = groupfileds[2];
            long numbers = value.getNumbers();
            String tablename = "memberinfoqushi";
            Map<String, String> dataMap = new HashMap<String, String>();
            dataMap.put("memberFlag",memberFlag);
            dataMap.put("timeinfoString",timeinfo);
            dataMap.put("numbers",numbers+"");
            Set<String> intFieldSet = new HashSet<String>();
            intFieldSet.add("numbers");
            ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
        }
    }
}
