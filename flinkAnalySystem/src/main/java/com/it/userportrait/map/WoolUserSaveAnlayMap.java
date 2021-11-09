package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.WoolEntity;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-10 15:40
 */
public class WoolUserSaveAnlayMap implements MapFunction<WoolEntity, WoolEntity> {
    @Override
    public WoolEntity map(WoolEntity woolEntity) throws Exception {
        String timeinfo = woolEntity.getTimeinfo();
        long  userid = woolEntity.getUserid();
        long numbers = woolEntity.getNumbers();
        String tableName = "WoolDayUserInfo";
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
        WoolEntity woolEntityResult = new WoolEntity();
        String groupField = "timeinfowool=="+timeinfo;
        woolEntity.setGroupField(groupField);
        woolEntity.setTimeinfo(timeinfo);
        if(numbers>5){
            String hasFlag = HbaseUtils2.getdata(tableName,rowkey,famliyname,"hasFlag");
            if(StringUtils.isBlank(hasFlag)){
                woolEntityResult.setNumbers(1l);
            }
            HbaseUtils2.putdata(tableName,rowkey,famliyname,"hasFlag","1");
            tableName =  "userinfo";
            String cloumnW = "wooluser";
            String rowkeykey = userid+"";
            Map<String,String> dataMap = new HashMap<String,String>();
            dataMap.put(timeinfo,numbers+"");
            String cloumnWTimes = "couponsTimes";
            HbaseUtils2.putdata(tableName,rowkeykey,famliyname,cloumnW,"1");
            HbaseUtils2.putdata(tableName,rowkeykey,famliyname,cloumnWTimes, JSONObject.toJSONString(dataMap));
        }

        return  woolEntityResult;

    }
}
