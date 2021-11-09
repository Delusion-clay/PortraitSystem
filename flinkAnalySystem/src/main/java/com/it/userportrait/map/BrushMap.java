package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.BrushEntity;
import com.it.userportrait.bean.Order;
import com.it.userportrait.util.AddressUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class BrushMap implements MapFunction<String, BrushEntity> {


    @Override
    public BrushEntity  map(String s) throws Exception {
        Order youfanorder = JSONObject.parseObject(s, Order.class);
        long userid = youfanorder.getUserid();
        String adress = youfanorder.getAdress();
        adress = AddressUtils.filterAddress(adress);
        BrushEntity brushEntity = new BrushEntity();
        brushEntity.setUserid(userid);
        brushEntity.setAddress(adress);
        long timeinfo = DateUtils.getCurrentMonthStart(System.currentTimeMillis());
        String timeinfoString = DateUtils.getDateString(timeinfo,"yyyyMM");
        brushEntity.setTimeinfoString(timeinfoString);
        brushEntity.setTimeinfo(timeinfo+"");
        String groupField = "brush=="+userid+"=="+adress+"=="+timeinfo;
        brushEntity.setGroupField(groupField);
        brushEntity.setNumbers(1l);
        return brushEntity;
    }
}
