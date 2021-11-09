package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.WoolEntity;
import com.it.userportrait.bean.Order;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Date;

public class WoolAnlayMap implements MapFunction<String, WoolEntity> {

    @Override
    public WoolEntity map(String s) throws Exception {
        Order youfanorder = JSONObject.parseObject(s, Order.class);
        long userid = youfanorder.getUserid();
        Date orderDate = youfanorder.getCreateTime();
        long orderDateLong = orderDate.getTime();
        long couponId = youfanorder.getCouponId();
        WoolEntity woolEntity = new WoolEntity();
        woolEntity.setUserid(userid);

        long timeinfo = DateUtils.getCurrentDayStart(orderDateLong);
        woolEntity.setTimeinfo(timeinfo+"");
        String groupField = "wooluser=="+timeinfo+"=="+userid;
        woolEntity.setGroupField(groupField);
        if(couponId>0){
            woolEntity.setNumbers(1l);
        }
        return woolEntity;
    }
}
