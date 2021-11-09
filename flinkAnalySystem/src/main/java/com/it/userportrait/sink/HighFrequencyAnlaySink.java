package com.it.userportrait.sink;

import com.it.userportrait.analy.BrushEntity;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.Map;

public class HighFrequencyAnlaySink implements SinkFunction<BrushEntity> {

    public void invoke(BrushEntity value, Context context) throws Exception {
        if(value != null) {
            String groupyField = value.getGroupField();
            String timeinfo = value.getTimeinfo();
            long  userid = value.getUserid();
            long numbers = value.getNumbers();
            String tableName = "highFrequencyMonth";
            String rowkey = userid+timeinfo;
            String famliyname = "info";
            String cloumn = "times";
            long brefore = 0l;
            String breforeString = HbaseUtils2.getdata(tableName,rowkey,famliyname,cloumn);
            if(StringUtils.isNotBlank(breforeString)){
                brefore = Long.valueOf(breforeString);
            }
            numbers+= brefore;
            HbaseUtils2.putdata(tableName,rowkey,famliyname,cloumn,numbers+"");
            if(numbers>6){
                tableName =  "userinfo";
                String cloumnF = "highFrequency";
                String cloumnFTimes = "highFrequencyTimes";
                HbaseUtils2.putdata(tableName,rowkey,famliyname,cloumnF,"1");
                HbaseUtils2.putdata(tableName,rowkey,famliyname,cloumnFTimes,numbers+"");
            }
        }
    }
}
