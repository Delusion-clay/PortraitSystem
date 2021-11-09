package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.WoolEntity;
import com.it.userportrait.bean.Order;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.Map;

public class WoolWarningAnlayMap implements MapFunction<String, WoolEntity> {

    @Override
    public WoolEntity map(String s) throws Exception {
        Order order = JSONObject.parseObject(s, Order.class);
        long userid = order.getUserid();
        long conpusId = order.getCouponId();
        long timeinfo = DateUtils.getCurrentHourStart(System.currentTimeMillis());
        WoolEntity woolEntity = new WoolEntity();
        woolEntity.setTimeinfo(timeinfo+"");
        woolEntity.setUserid(userid);
        woolEntity.setConpusId(conpusId);
        if(conpusId > 0){
            woolEntity.setNumbers(1l);
        }
        String groupField = "WoolWarning=="+userid+"=="+timeinfo+"=="+conpusId;
        woolEntity.setGroupField(groupField);
        return woolEntity;
    }
}
