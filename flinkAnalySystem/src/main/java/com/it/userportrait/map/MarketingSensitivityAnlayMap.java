package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.MarketingSensitivityEntity;
import com.it.userportrait.bean.Order;
import com.it.userportrait.log.AdvisterOpertor;
import com.it.userportrait.util.DateUtils;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.Map;

public class MarketingSensitivityAnlayMap implements MapFunction<JSONObject, MarketingSensitivityEntity> {
    public MarketingSensitivityEntity map(JSONObject s) throws Exception {
        String advis = s.getString("adviser");
        String orderinfo = s.getString("orderinfo");
        AdvisterOpertor advisterOpertor = JSONObject.parseObject(advis, AdvisterOpertor.class);
        int  advType = advisterOpertor.getDiviceType();
        long userid = advisterOpertor.getUserid();
        long advisterId = advisterOpertor.getAdviId();
        Order order = JSONObject.parseObject(orderinfo, Order.class);
        //非常敏感2或者点击5+ + 1 一般5+ 或者1+ 1  不敏感2
        int ordernums = 0;
        if(order != null){
            ordernums = 1;
        }
        int advernums = 1;
        MarketingSensitivityEntity marketingSensitivityEntity = new MarketingSensitivityEntity();
        marketingSensitivityEntity.setAdvernums(advernums);
        marketingSensitivityEntity.setOrdernums(ordernums);
        marketingSensitivityEntity.setUserid(userid);
        marketingSensitivityEntity.setAdvisterId(advisterId);
        long timeinfo = DateUtils.getCurrentHourStart(System.currentTimeMillis());
        String timeinfoString = DateUtils.getDateString(timeinfo,"yyyyMMddhh");
        marketingSensitivityEntity.setTimeinfo(timeinfo);
        marketingSensitivityEntity.setTimeinfoString(timeinfoString);
        String fieldGroup = "MarketingSensitivity=="+timeinfo+"=="+userid+"=="+advisterId;
        marketingSensitivityEntity.setGroupField(fieldGroup);
        marketingSensitivityEntity.setAdvType(advType);
        return marketingSensitivityEntity;
    }
}
