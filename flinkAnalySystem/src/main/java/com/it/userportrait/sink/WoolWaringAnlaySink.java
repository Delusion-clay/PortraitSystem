package com.it.userportrait.sink;

import com.it.userportrait.analy.WoolEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WoolWaringAnlaySink implements SinkFunction<WoolEntity> {
    @Override
    public void invoke(WoolEntity value, Context context) throws Exception {
        String timeinfo = value.getTimeinfo();
        long nubmers = value.getNumbers();
        long conpusId = value.getConpusId();
        long userid = value.getUserid();
        String timeinfoString = DateUtils.getDateString(Long.valueOf(timeinfo),"yyyyMMddhh");
        if(nubmers>20){//羊毛风险行为
            String tablename = "WoolWaringdayinfo";
            Map<String, String> dataMap = new HashMap<String, String>();
            dataMap.put("timeinfo",timeinfo);
            dataMap.put("conpusId",conpusId+"");
            dataMap.put("userids",userid+"");
            dataMap.put("nubmers",nubmers+"");
            dataMap.put("timeinfoString",timeinfoString);
            Set<String> intFieldSet = new HashSet<String>();
            intFieldSet.add("numbers");
            ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
            //发邮件
            //youfanEmail.sendMai("用户=="+user+"优惠券是："+counons+"=="+使用+times+“次”+“，有极大风险，请检查系统是否有漏洞”)
        }
    }
}
