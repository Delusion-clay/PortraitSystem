package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.BrushEntity;
import com.it.userportrait.bean.Order;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class HighFrequencyAnlayMap implements MapFunction<String, BrushEntity> {
    public BrushEntity map(String s) throws Exception {
        Order youfanorder = JSONObject.parseObject(s, Order.class);
        Date orderDate = youfanorder.getCreateTime();
        long currentOrderTime = orderDate.getTime();
        long userid = youfanorder.getUserid();
        BrushEntity brushEntityResult = new BrushEntity();
        String tablename = "highFrequencyUserinfo";
        String rowkey = userid+"";
        String famliyName = "info";
        String columnbre = "timeorderBre";
//        String columncurent = "timeorderCurrent";
        String bewtweent = "timebewtween";

        String breTime = HbaseUtils2.getdata(tablename,rowkey,famliyName,columnbre);
        if(StringUtils.isNotBlank(breTime)){
            long betweent = currentOrderTime - Long.valueOf(breTime);
            long days = betweent/1000*60*60*24l;
            if(days<2){
                brushEntityResult.setNumbers(1l);
            }
            HbaseUtils2.putdata(tablename,rowkey,famliyName,columnbre,currentOrderTime+"");
            HbaseUtils2.putdata(tablename,rowkey,famliyName,bewtweent,betweent+"");
        }else{
            HbaseUtils2.putdata(tablename,rowkey,famliyName,columnbre,currentOrderTime+"");
        }
        long monthDate = DateUtils.getCurrentMonthStart(System.currentTimeMillis());
        String groupField = "HighFrequency=="+userid+"=="+monthDate;
        brushEntityResult.setGroupField(groupField);
        brushEntityResult.setUserid(userid);
        brushEntityResult.setTimeinfo(monthDate+"");
        String timeinfoString = DateUtils.getDateString(monthDate,"yyyyMM");
        brushEntityResult.setTimeinfoString(timeinfoString);
        return brushEntityResult;
    }
}
